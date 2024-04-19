'''
Autor: Mijie Pang
Date: 2023-02-14 20:55:20
LastEditTime: 2024-04-10 20:36:50
Description: 
'''
import os
import sys
import logging
import numpy as np
from numpy.linalg import inv
import netCDF4 as nc
from datetime import datetime, timedelta

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
sys.path.append(os.path.join(main_dir, 'Assimilation'))

import system_lib as stl
import LE_obs_lib as leol
import LE_write_lib as lewl
import LE_output_lib as leopl
import Localization_lib as lol
import Assimilation_lib as asl


def main(Config: dict, **kwargs):

    ### *--- set the system status ---* ###
    project_start_run_time = datetime.now()
    home_dir = os.getcwd()
    Status = stl.edit_json(
        path=os.path.join(main_dir, 'Status.json'),
        new_dict={'assimilation': {
            'code': 10,
            'home_dir': home_dir
        }})

    logging.info('working file : %s' % (__file__))

    ### *--------------------------------------* ###
    ### *---       read configuration       ---* ###

    ### *--- initial the parameters ---* ###
    assimilation_scheme = Config['Assimilation']['scheme']['name']
    model_scheme = Config['Model']['scheme']['name']

    Ne = Config['Model'][model_scheme][
        'ensemble_number']  # N is the number is ensembles
    Nspec = Config['Model'][model_scheme]['nspec']
    Nlev = Config['Model'][model_scheme]['nlevel']
    Nlon = Config['Model'][model_scheme]['nlon']
    Nlat = Config['Model'][model_scheme]['nlat']
    Ns = Nlon * Nlat
    model_lon = np.arange(Config['Model'][model_scheme]['start_lon'],
                          Config['Model'][model_scheme]['end_lon'],
                          Config['Model'][model_scheme]['res_lon'])
    model_lat = np.arange(Config['Model'][model_scheme]['start_lat'],
                          Config['Model'][model_scheme]['end_lat'],
                          Config['Model'][model_scheme]['res_lat'])
    iteration_num = Config['Model'][model_scheme]['iteration_num']

    assimilation_time = datetime.strptime(
        Status['assimilation']['assimilation_time'], '%Y-%m-%d %H:%M:%S')
    execute_time = datetime.strptime(
        Config['Assimilation'][assimilation_scheme]['execute_time_point'],
        '%Y-%m-%d %H:%M:%S')

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

    ### *------------------------------------* ###
    ###                                        ###
    ###           Start Assimilation           ###
    ###                                        ###
    ### *------------------------------------* ###

    ### *--- configure the time and ensemble set ---* ###
    time_set_mark = Config['Assimilation'][assimilation_scheme]['time_set']
    ensemble_set = Config['Assimilation'][assimilation_scheme]['ensemble_set']
    ensemble_set = [
        int(ensemble_set[i_ensem]) for i_ensem in range(len(ensemble_set))
    ]
    idx = time_set_mark.index(0)
    Ne_extend = sum(ensemble_set)

    ### only do neighboring time at the first assimilation ###
    if assimilation_time == execute_time:

        time_set = [
            assimilation_time + timedelta(hours=int(time_set_mark[i_time]))
            for i_time in range(len(time_set_mark))
        ]
        run_id_read = [[
            'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
            for i_ensem in range(ensemble_set[idx])
        ] for i_time in range(len(time_set))]

        apply_neighboring = True

    else:

        time_set = [assimilation_time for i_time in range(len(time_set_mark))]
        run_id_read = [[
            't_%s_e_%02d' % (time_set_mark[i_time], i_ensem)
            for i_ensem in range(ensemble_set[i_time])
        ] for i_time in range(len(time_set))]
        apply_neighboring = False

    run_id_write = [[
        't_%s_e_%02d' % (time_set_mark[i_time], i_ensem)
        for i_ensem in range(ensemble_set[i_time])
    ] for i_time in range(len(time_set))]

    logging.debug(f'list of run id to read : {run_id_read}')
    logging.debug(f'list of time : {time_set}')
    logging.debug(f'list of ensemble set : {ensemble_set}')

    ### *--- initiate the variables ---* ###
    X_f_read = np.zeros([Ne_extend, Nspec, Nlev, Nlat, Nlon])

    ### *-------------------------------------------* ###
    ### *---   retrive ensemble model results    ---* ###

    for i_time in range(len(time_set)):
        for i_ensem in range(ensemble_set[i_time]):

            Ne_count = sum(ensemble_set[:i_time]) + i_ensem

            path = os.path.join(
                Config['Model'][model_scheme]['path']['model_output_path'],
                Config['Model'][model_scheme]['run_project'],
                run_id_read[i_time][i_ensem], 'restart', 'LE_%s_state_%s.nc' %
                (run_id_read[i_time][i_ensem],
                 time_set[i_time].strftime('%Y%m%d_%H%M')))

            with nc.Dataset(path) as nc_obj:
                X_f_read[Ne_count, :] = nc_obj.variables['c'][:]

    logging.info('%s ensemble priors received' % (Ne_extend))

    ### *--- calculate average vertical structure ---* ###

    # dust ratio for storing , dim : Nspec * Nlev * Ns
    dust_ratio_sfc2layers = np.zeros([len(time_set), Nspec, Nlev, Ns])
    x_f_dust_mean = np.zeros([Nspec, Nlev, Nlat, Nlon])
    for i_time in range(len(time_set)):

        x_f_dust_mean = np.mean(
            X_f_read[sum(ensemble_set[:i_time]):sum(ensemble_set[:i_time +
                                                                 1]), :],
            axis=0)
        dust_ratio_sfc2layers[i_time] = (
            x_f_dust_mean / np.sum(x_f_dust_mean, axis=0)[0, :, :]).reshape(
                [Nspec, Nlev, Ns])

    dust_ratio_sfc2layers[np.isnan(dust_ratio_sfc2layers)] = 0
    dust_ratio_sfc2layers[dust_ratio_sfc2layers > 9] = 9

    vertical_fraction = np.mean(np.sum(np.mean(dust_ratio_sfc2layers, axis=0),
                                       axis=0),
                                axis=-1)
    logging.info(f'sfc2layers vertical fraction : {vertical_fraction}')

    ### *--- save the temporary variables ---* ###
    if Config['Assimilation']['post_process']['save_variables']:

        x_f_3d = np.mean(np.sum(X_f_read, axis=1), axis=0)

        if Config['Assimilation']['post_process']['save_method'] == 'npy':

            asl.save2npy(dir_name=save_variables_dir,
                         variables={
                             'prior': np.sum(np.mean(X_f_read, axis=0),
                                             axis=0),
                         })

        elif Config['Assimilation']['post_process']['save_method'] == 'nc':

            var_output.save('prior_dust_3d', x_f_3d)
            var_output.save('prior_dust_all', X_f_read, Ne=Ne_extend)

    ### *-----------------------------------------* ###
    ### *---     read observations and map     ---* ###

    obs = leol.Observation(Config['Observation']['path'], 'bc_pm10',
                           Config['Observation']['bc_pm10'])
    obs.get_data(assimilation_time)
    obs.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
    obs.get_error('fraction', threshold=200, factor=0.1)

    R = np.diag((obs.error**2).reshape(-1))

    ### *---------------------------------------* ###
    ### *---      calculate Kalman Gain      ---* ###

    # convert [Ne_extend, Nspec, Nlev, Nlat, Nlon] to [Ne_extend, Ns]
    X_f_extend = np.sum(X_f_read[:, :, 0, :, :], axis=1)
    X_f_extend = X_f_extend.reshape([Ne_extend, Ns])
    x_f_mean = np.nanmean(X_f_extend, axis=0)
    X_pertubate = (X_f_extend - x_f_mean).T
    logging.debug(f'dim of X_pertubate : {X_pertubate.shape}')
    U = X_pertubate[obs.map_idx, :]
    logging.debug(f'dim of U : {U.shape}')

    if Config['Assimilation'][assimilation_scheme]['use_localization']:

        start_local = datetime.now()
        logging.info('Localization enabled, distance threshold : %s km' % (
            Config['Assimilation'][assimilation_scheme]['distance_threshold']))

        model_lon_meshed, model_lat_meshed = np.meshgrid(model_lon, model_lat)
        model_lon_meshed = np.ravel(model_lon_meshed)
        model_lat_meshed = np.ravel(model_lat_meshed)

        local = lol.Local_class(
            model_lon_meshed,
            model_lat_meshed,
            model_lon_meshed[obs.map_idx],
            model_lat_meshed[obs.map_idx],
            Config['Assimilation'][assimilation_scheme]['distance_threshold'],
        )
        L1 = local.calculate()  # dim : Ns * m
        logging.debug((f'Dims of Localization 1 : {L1.shape}'))

        local = lol.Local_class(
            model_lon_meshed[obs.map_idx], model_lat_meshed[obs.map_idx],
            model_lon_meshed[obs.map_idx], model_lat_meshed[obs.map_idx],
            Config['Assimilation'][assimilation_scheme]['distance_threshold'])
        L2 = local.calculate()  # dim : m * m
        logging.debug((f'Dims of Localization 2 : {L2.shape}'))

        logging.debug('localization took %.2f s' %
                      ((datetime.now() - start_local).total_seconds()))

        K = (np.multiply(X_pertubate @ U.T, L1) / (Ne_extend - 1)
             ) @ inv(np.multiply(U @ U.T, L2) / (Ne_extend - 1) + R)

    else:
        K = (X_pertubate @ U.T) / (Ne_extend - 1) @ inv(U @ U.T /
                                                        (Ne_extend - 1) + R)

    logging.info('Kalman Gain calculated')

    ### *---------------------------------------------------* ###
    ### *---  calculate poerteriors and write them back  ---* ###

    ### *--- convert to 3D and write back to restart file ---* ###
    if Config['Assimilation'][assimilation_scheme]['write_restart']:

        wr = lewl.WriteRestart(
            Config['Model'][model_scheme]['path']['model_output_path'],
            Config['Model'][model_scheme]['run_project'])

        X_a = np.zeros([Ns, Ne_extend])
        X_a_3d = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne_extend])

        for i_time in range(len(time_set)):
            for i_ensem in range(ensemble_set[i_time]):

                model_restart_path = os.path.join(
                    Config['Model'][model_scheme]['path']['model_output_path'],
                    Config['Model'][model_scheme]['run_project'],
                    run_id_write[i_time][i_ensem], 'restart')

                if not os.path.exists(model_restart_path):
                    os.makedirs(model_restart_path)

                n_count = sum(ensemble_set[:i_time]) + i_ensem

                y_i = obs.values + np.random.normal(
                    loc=0, scale=obs.error, size=(obs.m, 1))

                y_i = y_i.reshape([obs.m, 1])
                x_f_i = X_f_extend[n_count].reshape([Ns, 1])

                X_a[:,
                    n_count] = (x_f_i +
                                K @ (y_i - x_f_i[obs.map_idx, :])).reshape(-1)

                X_a_3d[:, :, :, :,
                       n_count] = (X_a[:, n_count].reshape(-1) *
                                   dust_ratio_sfc2layers[i_time]).reshape(
                                       [Nspec, Nlev, Nlat, Nlon])

                if apply_neighboring:

                    base_id = 'iter_%02d_ensem_00' % (iteration_num)
                    asl.rsync_file(
                        source_path=os.path.join(
                            Config['Model'][model_scheme]['path']
                            ['model_output_path'],
                            Config['Model'][model_scheme]['run_project'],
                            base_id, 'restart', 'LE_%s_state_%s.nc' %
                            (base_id,
                             assimilation_time.strftime('%Y%m%d_%H%M'))),
                        target_path=os.path.join(
                            Config['Model'][model_scheme]['path']
                            ['model_output_path'],
                            Config['Model'][model_scheme]['run_project'],
                            run_id_write[i_time][i_ensem], 'restart',
                            'LE_%s_state_%s.nc' %
                            (run_id_write[i_time][i_ensem],
                             assimilation_time.strftime('%Y%m%d_%H%M'))))

                wr.write(X_a_3d[:, :, :, :, n_count],
                         run_id_write[i_time][i_ensem], assimilation_time, 'c')

        var_output.save('posterior_dust_all', X_a_3d, Ne=Ne_extend)

        logging.info('Ensemble posteriors written')

    ### *---------------------------------* ###
    ### *---    Save the variables     ---* ###
    if Config['Assimilation']['post_process']['save_variables']:

        x_f_mean = x_f_mean.reshape([Ns, 1])
        x_a_mean = x_f_mean + K @ (obs.values - x_f_mean[obs.map_idx, :])

        x_a_3d = np.zeros([Nspec, Nlev, Ns])
        x_a_3d = x_a_mean.reshape(-1) * np.mean(dust_ratio_sfc2layers, axis=0)

        if Config['Assimilation']['post_process']['save_method'] == 'npy':

            asl.save2npy(dir_name=save_variables_dir,
                         variables={
                             'posterior': np.sum(np.mean(X_a_3d, axis=0),
                                                 axis=0),
                         })

        elif Config['Assimilation']['post_process']['save_method'] == 'nc':

            var_output.save('posterior_dust_3d', np.sum(x_a_3d, axis=0))

    ### *--------------------------------------* ###
    ### *---   tell main branch i am done   ---* ###
    logging.info('Assimilation finished, took {:.2f} s'.format(
        (datetime.now() - project_start_run_time).total_seconds()))
    stl.edit_json(path=os.path.join(main_dir, 'Status.json'),
                  new_dict={'assimilation': {
                      'code': 100
                  }})


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    main(Config)
