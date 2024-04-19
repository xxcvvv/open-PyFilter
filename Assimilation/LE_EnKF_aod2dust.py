'''
Autor: Mijie Pang
Date: 2023-11-02 15:25:15
LastEditTime: 2024-04-10 20:33:36
Description: 
State : Dust & AOD
Observation : AOD
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
import LE_write_lib as lewl
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

    ### *-------------------------------------------------* ###
    ### *---   Section 1 : initialize configurations   ---* ###
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

    dust_specs = ['dust_ff', 'dust_f', 'dust_c', 'dust_cc', 'dust_ccc']

    assimilation_time = datetime.strptime(
        Status['assimilation']['assimilation_time'], '%Y-%m-%d %H:%M:%S')

    ### initialize the saving variablse procedure ###
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
    # aod prior matrix
    X_f_aod_read = np.zeros([Nlat, Nlon, Ne])
    # dust prior matrix
    X_f_dust_read = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne])

    ### *-----------------------------------------------------* ###
    ### *---   Section 2 : retrive ensemble model priors   ---* ###
    mr = lerl.ModelReader(
        Config['Model'][model_scheme]['path']['model_output_path'],
        Config['Model'][model_scheme]['run_project'])

    # for i_ensem in range(Ne):

    #     run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)

    #     X_f_aod_read[:, :, i_ensem] = np.ravel(
    #         mr.read_output(Config['Model'][model_scheme]['run_project'],
    #                        run_id, 'aod2', assimilation_time, 'aod_550nm'))
    #     X_f_dust_read[:, :, :, :, i_ensem] = mr.read_restart(
    #         Config['Model'][model_scheme]['run_project'], run_id,
    #         assimilation_time)

    ### *--- parallel read by multi-threads ---* ###
    def read_model_output(run_id: str,
                          i_ensem: int,
                          X_f_read: np.ndarray,
                          data_type: str,
                          data_dir: str = None):
        if data_type == 'aod':
            X_f_read[:, :, i_ensem] = mr.read_output(run_id,
                                                     'aod2',
                                                     assimilation_time,
                                                     'aod_550nm',
                                                     output_dir=data_dir)
        elif data_type == 'dust':
            for i_spec, spec in enumerate(dust_specs):
                X_f_read[i_spec, :, :, :,
                         i_ensem] = mr.read_output(run_id,
                                                   'conc-3d',
                                                   assimilation_time,
                                                   spec,
                                                   output_dir=data_dir,
                                                   factor=1e9)

    def create_and_start_thread(target, args):
        thread = threading.Thread(target=target, args=args)
        thread.start()
        return thread

    threads = []
    for i_ensem in range(Ne):
        run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
        data_dir = os.path.join(Config['Info']['path']['output_path'],
                                Config['Model'][model_scheme]['run_project'],
                                'model_run', run_id, 'output')

        # Thread for AOD
        threads.append(
            create_and_start_thread(
                read_model_output,
                (run_id, i_ensem, X_f_aod_read, 'aod', data_dir)))

        # Thread for Dust
        threads.append(
            create_and_start_thread(
                read_model_output,
                (run_id, i_ensem, X_f_dust_read, 'dust', data_dir)))

    for thread in threads:
        thread.join()

    logging.info('%s ensemble priors received.' % (Ne))

    x_f_dust_sfc = np.sum(np.mean(X_f_dust_read, axis=-1),
                          axis=0)[0, :].reshape([Ns, 1])
    # ensemble column dust
    X_f_dust = np.sum(X_f_dust_read, axis=(0, 1)).reshape(Ns, Ne)
    # mean of column dust
    x_f_dust_mean = np.mean(X_f_dust, axis=-1).reshape([Ns, 1])
    # ensemble aod
    X_f_aod = X_f_aod_read.reshape([Ns, Ne])
    # mean of aod prior
    x_f_aod_mean = np.mean(X_f_aod_read, axis=-1).reshape([Ns, 1])

    # dust ratio in vertical, dim : Nlev * Ns
    x_f_dust_all_layers = np.sum(np.mean(X_f_dust_read, axis=-1), axis=0)
    dust_ratio_layers = x_f_dust_all_layers / np.sum(
        x_f_dust_all_layers, axis=0, keepdims=True)
    dust_ratio_layers = dust_ratio_layers.reshape([Nlev, Ns])
    dust_ratio_layers[np.isnan(dust_ratio_layers)] = 0
    logging.debug(
        f'layers vertical fraction : {np.mean(dust_ratio_layers,axis=-1)}')

    ### *--- save the ensemble prior ---* ###
    if Config['Assimilation']['post_process']['save_variables']:

        # save to netCDF format
        pool.apply_async(var_output.save,
                         args=('prior_aod', np.mean(X_f_aod_read, axis=-1)))
        pool.apply_async(var_output.save,
                         args=('prior_dust_3d',
                               np.sum(np.mean(X_f_dust_read, axis=-1),
                                      axis=0)))

    ### *-----------------------------------------* ###
    ### *---   Section 3 : read observations   ---* ###

    # obs = leol.Observation(Config['Observation']['path'], 'modis_dod',
    #                        Config['Observation']['modis_dod'])
    # obs.get_data(assimilation_time)
    # obs.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
    # obs.get_error('fraction', threshold=0.1, factor=0.3)

    obs = leol.Observation(Config['Observation']['path'], 'himawari_8_dod',
                           Config['Observation']['himawari_8_dod'])
    obs.get_data(assimilation_time)
    obs.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
    obs.get_error('fraction', threshold=0.1, factor=0.3)

    ### *---------------------------------------------* ###
    ### *---   Section 4 : calculate Kalman Gain   ---* ###

    ### *--- Localization ---* ###
    if Config['Assimilation'][assimilation_scheme]['use_localization']:

        start_local = datetime.now()
        logging.info('Localization enabled, distance threshold : %s km' % (
            Config['Assimilation'][assimilation_scheme]['distance_threshold']))

        model_lon_meshed, model_lat_meshed = np.meshgrid(model_lon, model_lat)
        model_lon_meshed, model_lat_meshed = np.ravel(
            model_lon_meshed), np.ravel(model_lat_meshed)

        local = lol.Localization(model_lon_meshed, model_lat_meshed,
                                 model_lon_meshed[obs.map_idx],
                                 model_lat_meshed[obs.map_idx])
        local.cal_distance()

        L1 = local.cal_correlation(**Config['Assimilation']
                                   [assimilation_scheme])  # dim : Ns * m
        logging.debug((f'Dims of Localization 1 : {L1.shape}'))

        local = lol.Localization(model_lon_meshed[obs.map_idx],
                                 model_lat_meshed[obs.map_idx],
                                 model_lon_meshed[obs.map_idx],
                                 model_lat_meshed[obs.map_idx])
        local.cal_distance()
        L2 = local.cal_correlation(**Config['Assimilation']
                                   [assimilation_scheme])  # dim : m * m
        logging.debug((f'Dims of Localization 2 : {L2.shape}'))

        logging.info('localization took %.2f s.' %
                     ((datetime.now() - start_local).total_seconds()))

    ### *--- Calculate the posterior ---* ###
    X_aod_pertubate = (X_f_aod - x_f_aod_mean).reshape([Ns, Ne])
    X_dust_pertubate = (X_f_dust - x_f_dust_mean).reshape([Ns, Ne])
    R = np.diag((obs.error**2).reshape(-1))
    U = X_aod_pertubate[obs.map_idx, :].reshape(-1, Ne)

    if Config['Assimilation'][assimilation_scheme]['use_localization']:

        K = (np.multiply(X_dust_pertubate @ U.T, L1) /
             (Ne - 1)) @ inv(np.multiply(U @ U.T, L2) / (Ne - 1) + R)

    else:

        K = (X_dust_pertubate @ U.T / (Ne - 1)) @ inv((U @ U.T) / (Ne - 1) + R)

    logging.info('Kalman Gain calculated.')

    ### *--------------------------------------------* ###
    ### *---   Section 5: update the posteriors   ---* ###

    ### write back to the Model restart files ###
    # if Config['Assimilation'][assimilation_scheme]['write_restart']:

    #     wr = lewl.WriteRestart(
    #         Config['Model'][model_scheme]['path']['model_output_path'],
    #         Config['Model'][model_scheme]['run_project'])

    #     ### calculate the posteriori ###
    #     for i_ensem in range(Ne):

    #         run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)

    #         y_i = obs.values + np.random.normal(
    #             loc=0, scale=obs.error, size=obs.m).reshape([obs.m, 1])
    #         x_f_i = X_f_dust[:, i_ensem].reshape([Ns, 1])

    #         x_a_dust = asl.calculate_posteriori(x_f=x_f_i, K=K, y=y_i, H=obs.H)
    #         x_a_dust_full = leal.allocate2full(x_a_dust,
    #                                            dust_ratio,
    #                                            Nspec=Nspec,
    #                                            Nlon=Nlon,
    #                                            Nlat=Nlat,
    #                                            Nlev=Nlev)
    #         wr.write(x_a_dust_full, run_id, assimilation_time, 'c')

    #     logging.info('Ensemble posteriors have been written.')

    ### *------------------------------------------* ###
    ### *---   Section 6 : save the variables   ---* ###
    x_a_dust_mean = x_f_dust_mean + K @ (obs.values -
                                         x_f_aod_mean[obs.map_idx])
    x_a_dust = x_a_dust_mean.reshape(-1) * dust_ratio_layers
    logging.info(
        f'Mean of prior : {np.mean(x_f_dust_sfc):.2f} and Mean of posterior : {np.mean(x_a_dust[0]):.2f}'
    )

    if Config['Assimilation']['post_process']['save_variables']:
        # save to netCDF format
        pool.apply_async(var_output.save, args=('posterior_dust_3d', x_a_dust))

    ### *------------------------------------------* ###
    ### *---   tell main branch i am finished   ---* ###
    logging.info('Assimilation finished, took %.2f s.' %
                 ((datetime.now() - timer0).total_seconds()))
    Status = stl.edit_json(path=status_path,
                           new_dict={'assimilation': {
                               'code': 100
                           }})

    ### *--- plot results ---* ###
    if Config['Assimilation']['post_process']['plot_results']:
        pool.apply_async(pa.aod_only,
                         args=(
                             obs.data,
                             ['MODIS DOD', 'modis_dod'],
                         ))
        pool.apply_async(pa.contour_with_scatter,
                         args=(
                             x_f_dust_sfc.reshape([Nlat, Nlon]),
                             ['Prior', 'prior'],
                         ))
        pool.apply_async(pa.contour_with_scatter,
                         args=(
                             x_a_dust[0, :].reshape([Nlat, Nlon]),
                             ['Posterior', 'posterior'],
                         ))
    pool.close()
    pool.join()


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    main(Config)
