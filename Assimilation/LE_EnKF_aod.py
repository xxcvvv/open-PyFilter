'''
Autor: Mijie Pang
Date: 2023-10-23 19:54:32
LastEditTime: 2024-04-10 20:36:25
Description: 
Assimilaion state : AOD
Assimilated observation : AOD
Assimilation target : AOD
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

home_dir = os.getcwd()


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

    #####################################################
    ###     Section 1 : initialize configurations     ###
    model_scheme = Config['Model']['scheme']['name']
    assimilation_scheme = Config['Assimilation']['scheme']['name']

    ### initialize the parameters ###
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

    ### initialize the saving variablse procedure ###
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

        var_output = leopl.Output(output_dir=save_variables_dir, Config=Config)

    ####################################################
    ###                                              ###
    ###              start Assimilation              ###
    ###                                              ###
    ####################################################

    ### initialize the variables ###
    X_f_aod = np.zeros([Ns, Ne])  # aod priori matrix
    X_f_dust = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne])  # dust priori matrix

    #######################################################
    ###    Section 2 : retrive ensemble model priors    ###
    mr = lerl.ModelReader(
        Config['Model'][model_scheme]['path']['model_output_path'])
    for i_ensem in range(Ne):

        run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
        X_f_aod[:, i_ensem] = np.ravel(
            mr.read_output(Config['Model'][model_scheme]['run_project'],
                           run_id, 'aod2', assimilation_time, 'aod_550nm'))
        X_f_dust[:, :, :, :, i_ensem] = mr.read_restart(
            Config['Model'][model_scheme]['run_project'], run_id,
            assimilation_time)

    logging.info('%s ensemble priors received.' % (Ne))

    x_f_dust_sfc = np.mean(np.sum(X_f_dust[:, 0, :, :, :], axis=0),
                           axis=2)  # mean of dust priori, dim : Ns * 1
    x_f_aod_mean = np.mean(X_f_aod, axis=1)  # mean of aod priori, dim : Ns * 1

    ### save the ensemble priori ###
    if Config['Assimilation']['post_process']['save_variables']:

        ### save to numpy format ###
        if Config['Assimilation']['post_process']['save_method'] == 'npy':
            asl.save2npy(dir_name=save_variables_dir,
                         variables={
                             'prior_aod_mean': x_f_aod_mean,
                         })

        ### save to netcdf format ###
        elif Config['Assimilation']['post_process']['save_method'] == 'nc':
            var_output.save('prior_aod', x_f_aod_mean)
            var_output.save('prior_dust_sfc', x_f_dust_sfc)

    #################################################
    ###       Section 3 : read observations       ###

    obs = leol.Observation(Config['Observation']['path'], 'modis_dod',
                           Config['Observation']['modis_dod'])
    obs.get_data(assimilation_time)
    obs.map2obs('nearest', model_lon, model_lat)
    obs.get_error('fraction', threshold=0.1, factor=0.3)

    logging.info('%s observations received from %s.' % (obs.m, 'modis'))

    ##################################################
    ###     Section 4 : calculate Kalman Gain      ###

    X_pertubate = X_f_aod - x_f_aod_mean.reshape([Ns, 1])
    U = obs.H @ X_pertubate

    ### Localization ###
    if Config['Assimilation'][assimilation_scheme]['use_localization']:

        start_local = datetime.now()
        logging.debug('Localization enabled, distance threshold : %s km' % (
            Config['Assimilation'][assimilation_scheme]['distance_threshold']))

        local = lol.Local_class(
            model_lon,
            model_lat,
            model_lon[obs.map_lon_idx],
            model_lat[obs.map_lat_idx],
            Config['Assimilation'][assimilation_scheme]['distance_threshold'],
            meshgrid1=True)
        L1 = local.calculate()  # dim : Ns * m
        logging.debug((f'Dims of Localization 1 : {L1.shape}'))

        local = lol.Local_class(
            model_lon[obs.map_lon_idx], model_lat[obs.map_lat_idx],
            model_lon[obs.map_lon_idx], model_lat[obs.map_lat_idx],
            Config['Assimilation'][assimilation_scheme]['distance_threshold'])
        L2 = local.calculate()  # dim : m * m
        logging.debug((f'Dims of Localization 2 : {L2.shape}'))

        logging.debug('localization took %.2f s.' %
                      ((datetime.now() - start_local).total_seconds()))

        K = (np.multiply(X_pertubate @ U.T, L1) /
             (Ne - 1)) @ inv(np.multiply(U @ U.T, L2) / (Ne - 1) + obs.O)

    else:

        K = (X_pertubate @ U.T / (Ne - 1)) @ inv((U @ U.T) / (Ne - 1) + obs.O)

    logging.info('Kalman Gain calculated.')

    ################################################
    ###     Section 5: update the posteriors     ###

    # if Config['Model'][model_scheme]['run_type'] == 'ensemble':

    #     ### calculate the posteriori ###
    #     X_a_aod = np.zeros([Ns, Ne])
    #     for i_ensem in range(Ne):

    #         y_i = obs.values + np.random.normal(
    #             loc=0, scale=obs.error, size=len(obs.values))

    #         y_i = y_i.reshape([obs.m, 1])
    #         x_f_i = X_f_aod[:, i_ensem].reshape([Ns, 1])

    #         X_a_aod[:, i_ensem] = asl.calculate_posteriori(x_f=x_f_i,
    #                                                        K=K,
    #                                                        y=y_i,
    #                                                        H=obs.H)
    #     X_a_dust = X_f_dust * (X_a_aod / X_f_aod)

    #     ### write back to the Model restart files ###
    #     if Config['Assimilation'][assimilation_scheme]['write_restart']:

    #         wr = lewl.WriteRestart(
    #             Config['Model'][model_scheme]['path']['model_output_path'],
    #             Config['Model'][model_scheme]['run_project'])

    #         for i_ensem in range(Ne):

    #             run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
    #             wr.write(X_a_dust[:, i_ensem].reshape([Nlat, Nlon]), run_id,
    #                      assimilation_time, 'c')

    ##############################################
    ###     Section 6 : save the variables     ###
    if Config['Assimilation']['post_process']['save_variables']:

        x_a_aod_mean = asl.calculate_posteriori(
            x_f=x_f_aod_mean.reshape([Ns, 1]),
            K=K,
            y=obs.values.reshape([obs.m, 1]),
            H=obs.H)
        # x_a_dust_mean = (x_a_aod_mean / x_f_aod_mean) * x_f_dust_mean

        if Config['Model'][model_scheme]['run_type'] == 'ensemble':

            ### save to numpy format ###
            if Config['Assimilation']['post_process']['save_method'] == 'npy':
                asl.save2npy(dir_name=save_variables_dir,
                             variables={
                                 'posterior':
                                 x_a_aod_mean.reshape([Nlat, Nlon]),
                             })

            ### save to netcdf format ###
            elif Config['Assimilation']['post_process']['save_method'] == 'nc':
                var_output.save('posterior_aod', x_a_aod_mean)
                # var_output.save('posterior_dust_sfc', x_a_dust_sfc)

    ##########################################
    ###   tell main branch i am finished   ###
    logging.info('Assimilation finished, took %.2f s.' %
                 ((datetime.now() - timer0).total_seconds()))
    stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 100}})


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    main(Config)
