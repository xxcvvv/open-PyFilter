'''
Autor: Mijie Pang
Date: 2024-01-05 21:36:21
LastEditTime: 2024-04-10 20:52:08
Description: 
State : Dust & AOD
Observation : PM10 & AOD
Target : Dust
'''
import os
import sys
import logging
import threading
import numpy as np
from numpy.linalg import inv
import multiprocessing as mp
from datetime import datetime

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
sys.path.append(os.path.join(main_dir, 'Assimilation'))

import system_lib as stl
import LE_obs_lib as leol
import LE_read_lib as lerl
import LE_output_lib as leopl
import Localization_lib as lol

from post_asml.LE_plot_lib import PlotAssimilation


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

    ### *-----------------------------------------------* ###
    ### *---  Section 1 : initialize configurations  ---* ###
    ### *-----------------------------------------------* ###

    model_scheme = Config['Model']['scheme']['name']
    assimilation_scheme = Config['Assimilation']['scheme']['name']

    assimilation_time = datetime.strptime(
        Status['assimilation']['assimilation_time'], '%Y-%m-%d %H:%M:%S')

    ### initialize the parameters ###
    Ne = Config['Model'][model_scheme][
        'ensemble_number']  # total number of ensembles
    Nspec = Config['Model'][model_scheme]['nspec']  # number of states
    Nlev = Config['Model'][model_scheme]['nlevel']  # number of vertical layers
    Nlon = Config['Model'][model_scheme]['nlon']  # number of longitude grids
    Nlat = Config['Model'][model_scheme]['nlat']  # number of latitude grids
    model_lon = np.arange(Config['Model'][model_scheme]['start_lon'],
                          Config['Model'][model_scheme]['end_lon'],
                          Config['Model'][model_scheme]['res_lon'])
    model_lat = np.arange(Config['Model'][model_scheme]['start_lat'],
                          Config['Model'][model_scheme]['end_lat'],
                          Config['Model'][model_scheme]['res_lat'])
    iteration_num = Config['Model'][model_scheme]['iteration_num']

    Ns = Nlon * Nlat  # number of states from model grids
    dust_specs = ['dust_ff', 'dust_f', 'dust_c', 'dust_cc', 'dust_ccc']

    ### *--- initialize some extra procedures ---* ###
    pool = mp.Pool(8)
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

    if Config['Assimilation']['post_process']['plot_results']:
        pa = PlotAssimilation(Config, Status)

    ### *----------------------------------------* ###
    ###                                            ###
    ###             Start Assimilation             ###
    ###                                            ###
    ### *----------------------------------------* ###

    ### *--- initialize the variables ---* ###
    # aod prior array
    X_f_aod_read = np.zeros([Nlat, Nlon, Ne])
    # dust prior array
    X_f_dust_read = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne])

    ### *--- read the model results ---* ###
    mr = lerl.ModelReader(
        Config['Model'][model_scheme]['path']['model_output_path'],
        Config['Model'][model_scheme]['run_project'])
    alt = mr.read_output('iter_00_ensem_00',
                         'conc-3d',
                         assimilation_time,
                         'altitude',
                         output_dir=os.path.join(
                             Config['Info']['path']['output_path'],
                             Config['Model'][model_scheme]['run_project'],
                             'model_run', 'iter_00_ensem_00', 'output'))

    ### *-- sequence read ---* ###
    # for i_ensem in range(Ne):

    #     run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
    #     model_output_dir = os.path.join(
    #         Config['Info']['path']['output_path'],
    #         Config['Model'][model_scheme]['run_project'], 'model_run', run_id,
    #         'output')

    #     X_f_aod_read[:, :,
    #                  i_ensem] = mr.read_output(run_id,
    #                                            'aod2',
    #                                            assimilation_time,
    #                                            'aod_550nm',
    #                                            output_dir=model_output_dir)

    #     for i_spec in range(len(dust_specs)):
    #         X_f_dust_read[i_spec, :, :, :, i_ensem] = mr.read_output(
    #             run_id,
    #             'conc-3d',
    #             assimilation_time,
    #             dust_specs[i_spec],
    #             output_dir=model_output_dir,
    #             factor=1e9)

    ### *--- parallel read by multi-threads ---* ###
    def read_model_output(run_id: str,
                          i_ensem: int,
                          X_f_read: np.ndarray,
                          data_type: str,
                          model_output_dir: str = None):
        if data_type == 'aod':
            X_f_read[:, :,
                     i_ensem] = mr.read_output(run_id,
                                               'aod2',
                                               assimilation_time,
                                               'aod_550nm',
                                               output_dir=model_output_dir)
        elif data_type == 'dust':
            for i_spec, spec in enumerate(dust_specs):
                X_f_read[i_spec, :, :, :,
                         i_ensem] = mr.read_output(run_id,
                                                   'conc-3d',
                                                   assimilation_time,
                                                   spec,
                                                   output_dir=model_output_dir,
                                                   factor=1e9)

    def create_and_start_thread(target, args):
        thread = threading.Thread(target=target, args=args)
        thread.start()
        return thread

    threads = []
    for i_ensem in range(Ne):
        run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
        model_output_dir = os.path.join(
            Config['Info']['path']['output_path'],
            Config['Model'][model_scheme]['run_project'], 'model_run', run_id,
            'output')

        # Thread for AOD
        threads.append(
            create_and_start_thread(
                read_model_output,
                (run_id, i_ensem, X_f_aod_read, 'aod', model_output_dir)))

        # Thread for Dust
        threads.append(
            create_and_start_thread(
                read_model_output,
                (run_id, i_ensem, X_f_dust_read, 'dust', model_output_dir)))

    for thread in threads:
        thread.join()

    logging.info('%s ensemble aod priors received.' % (Ne))
    logging.info('%s ensemble dust priors received.' % (Ne))

    ### *--- prepare the variables ---* ###
    x_f_dust_mean = np.mean(X_f_dust_read, axis=-1)
    # dust ratio in vertical, dim : Nlev * Ns
    x_f_layers = np.sum(x_f_dust_mean, axis=0)
    dust_ratio_layers = (x_f_layers / np.sum(x_f_layers, axis=0)).reshape(
        [Nlev, Ns])
    dust_ratio_layers[np.isnan(dust_ratio_layers)] = 0
    logging.debug(
        f'layers vertical fraction : {np.mean(dust_ratio_layers,axis=-1)}')

    # dust ratio for storing , dim : Nspec * Nlev * Ns
    dust_ratio_sfc2layers = (x_f_dust_mean /
                             np.sum(x_f_dust_mean, axis=0)[0, :, :]).reshape(
                                 [Nspec, Nlev, Ns])
    dust_ratio_sfc2layers[np.isnan(dust_ratio_sfc2layers)] = 0
    dust_ratio_sfc2layers[dust_ratio_sfc2layers > 9] = 9
    logging.debug(
        f'sfc2layers vertical fraction : {np.mean(np.sum(dust_ratio_sfc2layers,axis=0),axis=-1)}'
    )

    # dust prior
    X_f_dust = np.sum(X_f_dust_read, axis=0).reshape(Nlev, Ns, Ne)
    x_f_dust_mean = np.mean(X_f_dust, axis=-1).reshape([Nlev, Ns, 1])

    # aod prior, dim : Ns * Ne
    X_f_aod = X_f_aod_read.reshape([Ns, Ne])
    X_f_aod[np.isnan(X_f_aod)] = 0
    x_f_aod_mean = np.mean(X_f_aod, axis=-1).reshape([Ns, 1])

    ### *--- save the ensemble prior ---* ###
    if Config['Assimilation']['post_process']['save_variables']:

        # save to netCDF format
        pool.apply_async(var_output.save,
                         args=('prior_aod', np.mean(X_f_aod_read, axis=-1)))
        # pool.apply_async(var_output.save,
        #                  args=('prior_dust_3d',
        #                        x_f_dust_mean.reshape([Nlev, Nlat, Nlon])),
        #                  kwds={'altitude': alt})
        pool.apply_async(var_output.save,
                         args=('prior_dust', np.mean(X_f_dust_read, axis=-1)),
                         kwds={
                             'altitude': alt,
                             'std': np.std(np.sum(X_f_dust_read, axis=0),
                                           axis=-1)
                         })

    ### *---------------------------------------------------* ###
    ### *---        Section 3 : read observations        ---* ###
    ### *---------------------------------------------------* ###

    obs_dict = {}

    ### *--- BC-PM10 observation data ---* ###
    obs1 = leol.Observation(Config['Observation']['path'], 'bc_pm10',
                            Config['Observation']['bc_pm10'])
    obs1.get_data(assimilation_time)
    obs1.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
    obs1.get_error('fraction', threshold=200, factor=0.1)
    obs_dict['bc_pm10'] = obs1

    ### *--- MODIS DOD observation data ---* ###
    obs2 = leol.Observation(Config['Observation']['path'], 'modis_dod',
                            Config['Observation']['modis_dod'])
    obs2.get_data(assimilation_time)
    obs2.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
    obs2.get_error('fraction', threshold=0.1, factor=0.3)
    obs_dict['modis_dod'] = obs2

    ### *--- VIIRS DOD observation data ---* ###
    obs3 = leol.Observation(Config['Observation']['path'], 'viirs_dod',
                            Config['Observation']['viirs_dod'])
    obs3.get_data(assimilation_time)
    obs3.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
    obs3.get_error('fraction', threshold=0.1, factor=0.3)
    obs_dict['viirs_dod'] = obs3

    # plot the observations
    pool.apply_async(pa.pm10_only,
                     args=(
                         obs_dict['bc_pm10'].data,
                         ['BC-PM10', 'bc_pm10'],
                     ))
    pool.apply_async(pa.aod_only,
                     args=(
                         obs_dict['modis_dod'].data,
                         ['MODIS DOD', 'modis_dod'],
                     ))
    pool.apply_async(pa.aod_only,
                     args=(
                         obs_dict['viirs_dod'].data,
                         ['VIIRS DOD', 'viirs_dod'],
                     ))

    ### *------------------------------------------* ###
    ### *---  Section 4 : calculate Posteriors  ---* ###
    ### *------------------------------------------* ###

    ### *-- localization ---* ###
    if Config['Assimilation'][assimilation_scheme].get('use_localization',
                                                       False):

        start_local = datetime.now()
        logging.info('Localization enabled, distance threshold : %s km' % (
            Config['Assimilation'][assimilation_scheme]['distance_threshold']))

        model_lon_meshed, model_lat_meshed = np.meshgrid(model_lon, model_lat)
        model_lon_meshed, model_lat_meshed = np.ravel(
            model_lon_meshed), np.ravel(model_lat_meshed)

        L_dict = {'type1': [], 'type2': []}  # ground and AOD

        ### *--- for the ground observations ---* ###
        local = lol.Localization(
            model_lon_meshed,
            model_lat_meshed,
            model_lon_meshed[obs_dict['bc_pm10'].map_idx],
            model_lat_meshed[obs_dict['bc_pm10'].map_idx],
        )
        local.cal_distance()
        Correlation = local.cal_correlation(
            distance_threshold=Config['Assimilation'][assimilation_scheme]
            ['distance_threshold'])
        L_dict['type1'].append(Correlation)  # dim : Ns * m

        local = lol.Localization(model_lon_meshed[obs_dict['bc_pm10'].map_idx],
                                 model_lat_meshed[obs_dict['bc_pm10'].map_idx],
                                 model_lon_meshed[obs_dict['bc_pm10'].map_idx],
                                 model_lat_meshed[obs_dict['bc_pm10'].map_idx])
        local.cal_distance()
        Correlation = local.cal_correlation(
            distance_threshold=Config['Assimilation'][assimilation_scheme]
            ['distance_threshold'])
        L_dict['type1'].append(Correlation)  # dim : m * m

        logging.debug(
            f'Dims of Localization type 1 : {L_dict["type1"][0].shape} and {L_dict["type1"][1].shape}'
        )

        ### *--- for the AOD observations ---* ###
        local = lol.Localization(
            model_lon_meshed, model_lat_meshed,
            np.concatenate((model_lon_meshed[obs_dict['modis_dod'].map_idx],
                            model_lon_meshed[obs_dict['viirs_dod'].map_idx])),
            np.concatenate((model_lat_meshed[obs_dict['modis_dod'].map_idx],
                            model_lat_meshed[obs_dict['viirs_dod'].map_idx])))
        local.cal_distance()
        Correlation = local.cal_correlation(
            distance_threshold=Config['Assimilation'][assimilation_scheme]
            ['distance_threshold'])
        L_dict['type2'].append(Correlation)  # dim : Ns * m

        local = lol.Localization(
            np.concatenate((model_lon_meshed[obs_dict['modis_dod'].map_idx],
                            model_lon_meshed[obs_dict['viirs_dod'].map_idx])),
            np.concatenate((model_lat_meshed[obs_dict['modis_dod'].map_idx],
                            model_lat_meshed[obs_dict['viirs_dod'].map_idx])),
            np.concatenate((model_lon_meshed[obs_dict['modis_dod'].map_idx],
                            model_lon_meshed[obs_dict['viirs_dod'].map_idx])),
            np.concatenate((model_lat_meshed[obs_dict['modis_dod'].map_idx],
                            model_lat_meshed[obs_dict['viirs_dod'].map_idx])))
        local.cal_distance()
        Correlation = local.cal_correlation(
            distance_threshold=Config['Assimilation'][assimilation_scheme]
            ['distance_threshold'])
        L_dict['type2'].append(Correlation)  # dim : m * m
        logging.debug(
            f'Dims of Localization type 2 : {L_dict["type2"][0].shape} and {L_dict["type2"][1].shape}'
        )
        logging.info('Localization took %.2f s' %
                     ((datetime.now() - start_local).total_seconds()))

    start_cal = datetime.now()

    ### *------------------------------------* ###
    ### *---   Assimilate the AOD first   ---* ###

    ### *--- gather the pertubation matrix ---* ###
    # dim : n * N
    X_aod_pertubate = (X_f_aod - x_f_aod_mean).reshape([Ns, Ne])
    X_f_dust_all_layers = np.sum(X_f_dust, axis=0)
    x_f_dust_mean_all_layers = np.sum(x_f_dust_mean, axis=0)
    X_dust_pertubate_all_layers = (X_f_dust_all_layers -
                                   x_f_dust_mean_all_layers).reshape([Ns, Ne])

    ### *--- gather the U matrix ---* ###
    # dim : m * N
    U_modis = X_aod_pertubate[obs_dict['modis_dod'].map_idx, :].reshape(-1, Ne)
    U_viirs = X_aod_pertubate[obs_dict['viirs_dod'].map_idx, :].reshape(-1, Ne)
    U = np.concatenate((U_modis, U_viirs), axis=0)

    ### *--- gather the observations ---* ###
    # dim : m * 1
    value_modis = obs_dict['modis_dod'].values.reshape(-1, 1)
    value_viirs = obs_dict['viirs_dod'].values.reshape(-1, 1)
    y = np.concatenate((value_modis, value_viirs), axis=0)

    ### *--- gather the obervational error ---* ###
    # dim : m * 1
    error_modis = obs_dict['modis_dod'].error.reshape(-1, 1)
    error_viirs = obs_dict['viirs_dod'].error.reshape(-1, 1)
    obs_error = np.concatenate((error_modis, error_viirs), axis=0)

    ### *--- gather the innovation ---* ###
    # dim : m * 1
    innovation_ensemble = y - np.concatenate(
        (X_f_aod[obs_dict['modis_dod'].map_idx, :],
         X_f_aod[obs_dict['viirs_dod'].map_idx, :]))
    innovation_mean = y - np.concatenate(
        (x_f_aod_mean[obs_dict['modis_dod'].map_idx],
         x_f_aod_mean[obs_dict['viirs_dod'].map_idx]))

    ### *--- calculate the Kalman Gain and Posterior ---* ###
    R = np.diag((obs_error**2).reshape(-1))
    if Config['Assimilation'][assimilation_scheme].get('use_localization',
                                                       False):
        K = L_dict['type2'][0] * (X_dust_pertubate_all_layers @ U.T) / (
            Ne - 1) @ inv(L_dict['type2'][1] * (U @ U.T) / (Ne - 1) + R)
    else:
        K = (X_dust_pertubate_all_layers @ U.T / (Ne - 1)) @ inv((U @ U.T) /
                                                                 (Ne - 1) + R)

    epsilon = np.random.normal(loc=0,
                               scale=obs_error,
                               size=(len(obs_error), Ne))

    X_a_dust_all_layers = (X_f_dust_all_layers +
                           K @ (innovation_ensemble + epsilon)).astype(float)
    x_a_dust_all_layers = (x_f_dust_mean_all_layers +
                           K @ innovation_mean).astype(float)

    # transform this posterior into the prior
    # for the next ground observation assimilation
    x_f_dust = np.zeros([Nlev, Ns])
    X_f_dust = np.zeros([Nlev, Ns, Ne])

    x_f_dust = x_a_dust_all_layers.reshape(-1) * dust_ratio_layers
    for i_ensem in range(Ne):
        X_f_dust[:, :,
                 i_ensem] = X_a_dust_all_layers[:, i_ensem] * dust_ratio_layers

    ### *---------------------------------------------------* ###
    ### *---   Assimilate the ground observations then   ---* ###

    x_f_dust_sfc = x_f_dust[0, :].reshape([Ns, 1])
    X_f_dust_sfc = X_f_dust[0, :, :].reshape([Ns, Ne])

    ### *--- gather the pertubation matrix ---* ###
    # dim : n * N
    X_dust_sfc_pertubate = (X_f_dust_sfc - x_f_dust_sfc).reshape([Ns, Ne])

    ### *--- gather the U matrix ---* ###
    # dim : m * N
    U_dust = X_dust_sfc_pertubate[obs_dict['bc_pm10'].map_idx, :].reshape(
        -1, Ne)
    U = U_dust

    ### *--- gather the observations ---* ###
    # dim : m * 1
    value_dust = obs_dict['bc_pm10'].values.reshape(-1, 1)
    y = value_dust

    ### *--- gather the obervational error ---* ###
    # dim : m * 1
    error_dust = obs_dict['bc_pm10'].error.reshape(-1, 1)
    obs_error = error_dust

    ### *--- gather the innovation ---* ###
    # dim : m * 1
    innovation_mean = y - x_f_dust_sfc[obs_dict['bc_pm10'].map_idx]

    ### *--- calculate the Kalman Gain and Posterior ---* ###
    R = np.diag((obs_error**2).reshape(-1))
    if Config['Assimilation'][assimilation_scheme].get('use_localization',
                                                       False):
        K = L_dict['type1'][0] * (X_dust_sfc_pertubate @ U.T) / (
            Ne - 1) @ inv(L_dict['type1'][1] * (U @ U.T) / (Ne - 1) + R)
    else:
        K = (X_dust_sfc_pertubate @ U.T / (Ne - 1)) @ inv((U @ U.T) /
                                                          (Ne - 1) + R)

    x_a_dust_sfc = (x_f_dust_sfc + K @ innovation_mean).astype(float)

    logging.info('Posteriors calculation finished, took %.2f s' %
                 ((datetime.now() - start_cal).total_seconds()))

    ### *--- restore the dust full structure ---* ###
    x_a_dust = x_a_dust_sfc.reshape(-1) * dust_ratio_sfc2layers
    x_a_dust[np.logical_or(np.isnan(x_a_dust), x_a_dust <= 1e-9)] = 0
    logging.info('Mean of posterior : %.2f and Mean of prior : %.2f' %
                 (np.mean(np.sum(x_a_dust, axis=0)), np.mean(x_f_dust_mean)))

    ### *--------------------------------------------* ###
    ### *--- Section 5 : Post-process the results ---* ###
    ### *--------------------------------------------* ###

    ### *--- save the posterior ---* ###
    if Config['Assimilation']['post_process']['save_variables']:

        # save to netCDF format
        # pool.apply_async(var_output.save,
        #                  args=('posterior_dust_3d', np.sum(x_a_dust, axis=0)),
        #                  kwds={'altitude': alt})
        pool.apply_async(var_output.save,
                         args=('posterior_dust', x_a_dust),
                         kwds={'altitude': alt})

    ### *--------------------------------------* ###
    ### *--- tell main branch i am finished ---* ###
    logging.info('Assimilation finished, took %.2f s.' %
                 ((datetime.now() - timer0).total_seconds()))
    stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 100}})

    ### *--- plot the results ---* ###
    pool.apply_async(pa.contour_with_scatter,
                     args=(
                         x_f_dust_sfc.reshape([Nlat, Nlon]),
                         ['Prior', 'prior'],
                     ),
                     kwds={'obs_data': obs_dict['bc_pm10'].data})
    pool.apply_async(pa.contour_with_scatter,
                     args=(
                         x_a_dust_sfc.reshape([Nlat, Nlon]),
                         ['Posterior', 'posterior'],
                     ),
                     kwds={'obs_data': obs_dict['bc_pm10'].data})
    pool.close()
    pool.join()


if __name__ == '__main__':

    stl.Logging(log_level='DEBUG')
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    main(Config)
