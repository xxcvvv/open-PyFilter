'''
Autor: Mijie Pang
Date: 2023-02-16 16:37:57
LastEditTime: 2024-04-10 20:34:52
Description: 
Assimilaion state : Dust
Assimilated observation : PM10
Assimilation target : Dust
'''
import os
import sys
import logging
import numpy as np
from numpy.linalg import inv
from datetime import datetime

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
sys.path.append(os.path.join(main_dir, 'Assimilation'))

import system_lib as stl
import LE_obs_lib as leol
import LE_read_lib as lerl
import LE_write_lib as lewl
import LE_output_lib as leopl
import Localization_lib as lol
import Assimilation_lib as asl


def main(Config: dict, **kwargs):

    ### *--- set the system status ---* ###
    timer0 = datetime.now()
    home_dir = os.getcwd()
    status_path = os.path.join(main_dir, 'Status.json')
    Status = stl.edit_json(
        path=status_path,
        new_dict={'assimilation': {
            'code': 10,
            'home_dir': home_dir
        }})

    ### some debug information ###
    logging.info('Working directory : %s' % (home_dir))

    ### *---------------------------------------------* ###
    ### *--- Section 1 : initialize configurations ---* ###
    model_scheme = Config['Model']['scheme']['name']
    assimilation_scheme = Config['Assimilation']['scheme']['name']

    ### *--- initialize the parameters ---* ###
    Ne = Config['Model'][model_scheme][
        'ensemble_number']  # total number of ensembles
    Nspec = Config['Model'][model_scheme]['nspec']  # number of states
    Nlev = Config['Model'][model_scheme]['nlevel']  # number of vertical layers
    Nlon = Config['Model'][model_scheme]['nlon']  # number of longitude grids
    Nlat = Config['Model'][model_scheme]['nlat']  # number of latitude grids
    Ns = Nlon * Nlat  # total number of states
    model_lon = np.arange(Config['Model'][model_scheme]['start_lon'],
                          Config['Model'][model_scheme]['end_lon'],
                          Config['Model'][model_scheme]['res_lon'])
    model_lat = np.arange(Config['Model'][model_scheme]['start_lat'],
                          Config['Model'][model_scheme]['end_lat'],
                          Config['Model'][model_scheme]['res_lat'])
    iteration_num = Config['Model'][model_scheme]['iteration_num']

    ### *--- initiate the saving variablse procedure ---* ###
    assimilation_time = datetime.strptime(
        Status['assimilation']['assimilation_time'], '%Y-%m-%d %H:%M:%S')

    if Config['Assimilation']['post_process']['save_variables']:
        save_variables_dir = os.path.join(
            Config['Info']['path']['output_path'],
            Config['Model'][model_scheme]['run_project'],
            Config['Assimilation'][assimilation_scheme]['project_name'],
            'analysis', assimilation_time.strftime('%Y%m%d_%H%M'))
        if not os.path.exists(save_variables_dir):
            os.makedirs(save_variables_dir)

        var_output = leopl.Output(save_variables_dir,
                                  Config['Model'][model_scheme], **Config)

    logging.debug('working directory : %s' % (home_dir))

    ### *------------------------------------------* ###
    ###                                              ###
    ###              start Assimilation              ###
    ###                                              ###
    ### *------------------------------------------* ###

    ### *--- initialize the variables ---* ###
    X_f_3d = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne])  # 3d prior matrix

    ### *-------------------------------------------------* ###
    ### *--- Section 2 : retrive ensemble model priors ---* ###

    ### *--- read the model simulation ---* ###
    mr = lerl.ModelReader(
        Config['Model'][model_scheme]['path']['model_output_path'])

    for i_ensem in range(Ne):

        run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
        X_f_3d[:, :, :, :, i_ensem] = mr.read_restart(
            Config['Model'][model_scheme]['run_project'], run_id,
            assimilation_time)

    logging.info('%s ensemble priors received' % (Ne))

    ### *--- process the variables ---* ###
    # ensemble ground field, dim : Ns * Ne
    X_f = np.sum(X_f_3d[:, 0, :, :, :], axis=0).reshape(Ns, Ne)
    # mean of prior, dim : Ns * 1
    x_f_mean = np.mean(X_f, axis=1).reshape(-1, 1)
    # 3d prior, dim : Nlev * Nlat * Nlon
    x_f_3d = np.mean(np.sum(X_f_3d, axis=0), axis=-1)

    # dust ratio for storing , dim : Nspec * Nlev * Ns
    x_f_mean_tmp = np.mean(X_f_3d, -1)
    dust_ratio_sfc2layers = (x_f_mean_tmp /
                             np.sum(x_f_mean_tmp, axis=0)[0, :, :])
    dust_ratio_sfc2layers = dust_ratio_sfc2layers.reshape([Nspec, Nlev, Ns])
    dust_ratio_sfc2layers[np.isnan(dust_ratio_sfc2layers)] = 0
    dust_ratio_sfc2layers[dust_ratio_sfc2layers > 9] = 9

    logging.debug('sfc2layers vertical fraction : %s' %
                  (np.mean(np.sum(dust_ratio_sfc2layers, axis=0), axis=-1)))

    ### *--- save the ensemble prior ---* ###
    if Config['Assimilation']['post_process']['save_variables']:

        ### save to numpy format ###
        if Config['Assimilation']['post_process']['save_method'] == 'npy':
            asl.save2npy(dir_name=save_variables_dir,
                         variables={
                             'prior': x_f_mean.reshape([Nlat, Nlon]),
                         })

        ### save to netcdf format ###
        elif Config['Assimilation']['post_process']['save_method'] == 'nc':
            var_output.save('prior_dust_3d', x_f_3d)

    ### *-------------------------------------* ###
    ### *--- Section 3 : read observations ---* ###

    obs = leol.Observation(Config['Observation']['path'], 'bc_pm10',
                           Config['Observation']['bc_pm10'])
    obs.get_data(assimilation_time)
    obs.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
    obs.get_error('fraction', threshold=200, factor=0.1)

    R = np.diag((obs.error**2).reshape(-1))

    ### *-----------------------------------------* ###
    ### *--- Section 4 : calculate Kalman Gain ---* ###

    X_pertubate = X_f - x_f_mean
    U = X_pertubate[obs.map_idx, :]
    logging.debug(f'shape of U : {U.shape}')

    ### *--- Localization ---* ###
    if Config['Assimilation'][assimilation_scheme]['use_localization']:

        start_local = datetime.now()
        logging.info('Localization enabled, distance threshold : %s km' % (
            Config['Assimilation'][assimilation_scheme]['distance_threshold']))

        model_lon_meshed, model_lat_meshed = np.meshgrid(model_lon, model_lat)
        model_lon_meshed = np.ravel(model_lon_meshed)
        model_lat_meshed = np.ravel(model_lat_meshed)

        local = lol.Local_class(
            model_lon_meshed, model_lat_meshed, model_lon_meshed[obs.map_idx],
            model_lat_meshed[obs.map_idx],
            Config['Assimilation'][assimilation_scheme]['distance_threshold'])
        L1 = local.calculate()  # dim : Ns * m
        logging.debug((f'Dims of Localization 1 : {L1.shape}'))

        local = lol.Local_class(
            model_lon_meshed[obs.map_idx], model_lat_meshed[obs.map_idx],
            model_lon_meshed[obs.map_idx], model_lat_meshed[obs.map_idx],
            Config['Assimilation'][assimilation_scheme]['distance_threshold'])
        L2 = local.calculate()  # dim : m * m
        logging.debug((f'Dims of Localization 2 : {L2.shape}'))

        logging.info('localization took %.2f s' %
                     ((datetime.now() - start_local).total_seconds()))

        K = L1 * (X_pertubate @ U.T) / (Ne - 1) @ inv(L2 * (U @ U.T) /
                                                      (Ne - 1) + R)

    else:

        K = (X_pertubate @ U.T / (Ne - 1)) @ inv((U @ U.T) / (Ne - 1) + R)

    logging.info('Kalman Gain calculated')

    ### *----------------------------------------* ###
    ### *--- Section 5: update the posteriors ---* ###

    if Config['Model'][model_scheme]['run_type'] == 'ensemble':

        ### *--- calculate the posterior ---* ###
        X_a = np.zeros([Ns, Ne])
        X_a_3d = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne])

        wr = lewl.WriteRestart(
            Config['Model'][model_scheme]['path']['model_output_path'],
            Config['Model'][model_scheme]['run_project'])

        for i_ensem in range(Ne):

            run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)

            y_i = obs.values + np.random.normal(
                loc=0, scale=obs.error, size=(obs.m, 1))

            y_i = y_i.reshape([obs.m, 1])
            x_f_i = X_f[:, i_ensem].reshape([Ns, 1])

            X_a[:, i_ensem] = (x_f_i +
                               K @ (y_i - x_f_i[obs.map_idx, :])).reshape(-1)

            X_a_3d[:, :, :, :, i_ensem] = (X_a[:, i_ensem].reshape(-1) *
                                           dust_ratio_sfc2layers).reshape(
                                               [Nspec, Nlev, Nlat, Nlon])

            ### *--- write back to the Model restart files ---* ###
            if Config['Assimilation'][assimilation_scheme]['write_restart']:

                wr.write(X_a_3d[:, :, :, :, i_ensem], run_id,
                         assimilation_time, 'c')

    ### *--------------------------------------* ###
    ### *--- Section 6 : save the variables ---* ###
    if Config['Assimilation']['post_process']['save_variables']:

        x_a_mean = (x_f_mean + K @ (obs.values - x_f_mean[obs.map_idx, :]))
        x_a_mean = x_a_mean.reshape([Nlat, Nlon])

        x_a_3d = x_a_mean.reshape(-1) * dust_ratio_sfc2layers
        logging.debug(f'dims of x_a_3d : {x_a_3d.shape}')

        if Config['Model'][model_scheme]['run_type'] == 'ensemble':

            ### save to numpy format ###
            if Config['Assimilation']['post_process']['save_method'] == 'npy':

                asl.save2npy(dir_name=save_variables_dir,
                             variables={
                                 'posteriori': x_a_mean.reshape([Nlat, Nlon]),
                             })

            ### save to netcdf format ###
            elif Config['Assimilation']['post_process']['save_method'] == 'nc':
                var_output.save('posterior_dust_3d', np.sum(x_a_3d, axis=0))

    ### *----------------------------------------* ###
    ### *---  tell main branch i am finished  ---* ###
    logging.info('Assimilation finished, took %.2f s' %
                 ((datetime.now() - timer0).total_seconds()))
    stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 100}})


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    main(**Config)
