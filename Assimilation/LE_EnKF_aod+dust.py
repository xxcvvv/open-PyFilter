'''
Autor: Mijie Pang
Date: 2023-11-05 14:10:32
LastEditTime: 2023-12-19 15:50:17
Description: 
'''
import os
import sys
import numpy as np
from numpy.linalg import inv
import multiprocessing as mp
from datetime import datetime

import LE_asml_lib as leal
import LE_read_lib as lerl
import LE_obs_lib as leol
import LE_output_lib as leopl
import LE_write_lib as lewl
import Assimilation_lib as asl
import Localization_lib as lol

sys.path.append('../')
import system_lib as stl

home_dir = os.getcwd()

config_dir = '../config'
status_path = '../Status.json'

### *-----------------------------------------------* ###
### *---  Section 1 : initialize configurations  ---* ###
### *-----------------------------------------------* ###

Config = stl.read_json_dict(config_dir, 'Assimilation.json', 'Model.json',
                            'Observation.json', 'Info.json')
Status = stl.read_json(status_path)

model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']

### initialize the system log ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

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

    var_output = leopl.Output(save_variables_dir, Config)

### set the system status ###
timer0 = datetime.now()
stl.edit_json(path=status_path,
              new_dict={'assimilation': {
                  'code': 10,
                  'home_dir': home_dir
              }})

### some debug information ###
system_log.debug('Working directory : %s' % (home_dir))

####################################################
###                                              ###
###              start Assimilation              ###
###                                              ###
####################################################

### initialize the variables ###
X_f_aod_full = np.zeros([Nlat, Nlon, Ne])  # aod priori matrix
X_f_dust_full = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne])  # dust priori matrix

### *-------------------------------------------------* ###
### *--- Section 2 : retrive ensemble model priors ---* ###
### *-------------------------------------------------* ###

### *--- read the model results ---* ###
mr = lerl.Model_Reader(
    Config['Model'][model_scheme]['path']['model_output_path'])

for i_ensem in range(Ne):
    run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
    X_f_aod_full[:, :, i_ensem] = mr.read_output(
        Config['Model'][model_scheme]['run_project'], run_id, 'aod2',
        assimilation_time, 'aod_550nm')

system_log.info('%s ensemble aod priors received.' % (Ne))

for i_ensem in range(Ne):
    run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
    X_f_dust_full[:, :, :, :, i_ensem] = mr.read_restart(
        Config['Model'][model_scheme]['run_project'], run_id,
        assimilation_time)

system_log.info('%s ensemble dust priors received.' % (Ne))

### *--- prepare the variables ---* ###
# ensemble dust
X_f_dust = np.sum(X_f_dust_full, axis=0).reshape(Nlev, Ns, Ne)
# mean of dust
x_f_dust_mean = np.mean(X_f_dust, axis=-1).reshape([Nlev, Ns, 1])

# dust ratio
dust_ratio = np.zeros([Nspec, Nlev, Nlat, Nlon])
X_f_dust_mean = np.mean(X_f_dust_full, axis=-1)
# dim : Nspec * Nlev * Nlat * Nlon
dust_ratio = X_f_dust_mean / np.sum(X_f_dust_mean, axis=(0, 1), keepdims=True)
dust_ratio[np.isnan(dust_ratio)] = 0
# dust ratio in vertical, dim : Nlev * Ns
dust_ratio_layers = np.sum(dust_ratio, axis=0).reshape([Nlev, Ns])

### split the aod in the vertical dicrction ###
# aod prior, dim : Nlev, Ns, Ne
X_f_aod = np.zeros([Nlev, Ns, Ne])
for i_ensem in range(Ne):
    X_f_aod[:, :, i_ensem] = (X_f_aod_full[:, :, i_ensem].reshape([1, Ns]) *
                              dust_ratio_layers)
X_f_aod[np.isnan(X_f_aod)] = 0

# mean of aod priori
x_f_aod_mean = np.mean(X_f_aod, axis=-1).reshape([Nlev, Ns, 1])

### *--- save the ensemble priori ---* ###
if Config['Assimilation']['post_process']['save_variables']:

    ### save to numpy format ###
    if Config['Assimilation']['post_process']['save_method'] == 'npy':
        asl.save2npy(dir_name=save_variables_dir,
                     variables={
                         'prior_aod_mean': x_f_aod_mean,
                     })

    ### save to netcdf format ###
    elif Config['Assimilation']['post_process']['save_method'] == 'nc':
        var_output.save('prior_aod',
                        np.sum(x_f_aod_mean, axis=0).reshape([Nlat, Nlon]))
        var_output.save('prior_dust_sfc',
                        x_f_dust_mean[0, :, 0].reshape([Nlat, Nlon]))

### *---------------------------------------* ###
### *---  Section 3 : read observations  ---* ###
### *---------------------------------------* ###
obs = leol.Observations(Config['Observation']['path'])

### *--- BC-PM10 observation data ---* ###
obs.get_data('bc_pm10', assimilation_time, **Config['Observation']['bc_pm10'])
obs.map2obs('nearest', model_lon, model_lat)
obs.get_error('fraction', threshold=200, factor=0.1)

### *--- MODIS DOD observation data ---* ###
obs.get_data('modis_dod', assimilation_time,
             **Config['Observation']['modis_dod'])
obs.map2obs('nearest', model_lon, model_lat)
obs.get_error('fraction',
              threshold=0.1,
              factor=0.3,
              layering=dust_ratio_layers.reshape([Nlev, Ns]))

### *--- VIIRS DOD observation data ---* ###
obs.get_data('viirs_dod', assimilation_time,
             **Config['Observation']['viirs_dod'])
obs.map2obs('nearest', model_lon, model_lat)
obs.get_error('fraction',
              threshold=0.1,
              factor=0.3,
              layering=dust_ratio_layers.reshape([Nlev, Ns]))

### *------------------------------------------* ###
### *---  Section 4 : calculate Posteriors  ---* ###
### *------------------------------------------* ###

### *-- localization ---* ###
if Config['Assimilation'][assimilation_scheme]['use_localization']:

    start_local = datetime.now()
    system_log.debug(
        'Localization enabled, distance threshold : %s km' %
        (Config['Assimilation'][assimilation_scheme]['distance_threshold']))
    L1 = []
    L2 = []
    # on the ground level
    map_lon_idx, map_lat_idx = obs.gather_map_idx_seperate(
        'bc_pm10', 'modis_dod', 'viirs_dod')
    local = lol.Local_class(
        model_lon,
        model_lat,
        model_lon[map_lon_idx],
        model_lat[map_lat_idx],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'],
        meshgrid1=True)
    L1.append(local.calculate())  # dim : Ns * m
    system_log.debug((f'Dims of Localization 1 : {L1[0].shape}'))

    local = lol.Local_class(
        model_lon[map_lon_idx], model_lat[map_lat_idx], model_lon[map_lon_idx],
        model_lat[map_lat_idx],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'])
    L2.append(local.calculate())  # dim : m * m
    system_log.debug((f'Dims of Localization 2 : {L2[0].shape}'))

    # on the upper levels
    map_lon_idx, map_lat_idx = obs.gather_map_idx_seperate(
        'modis_dod', 'viirs_dod')
    local = lol.Local_class(
        model_lon,
        model_lat,
        model_lon[map_lon_idx],
        model_lat[map_lat_idx],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'],
        meshgrid1=True)
    L1.append(local.calculate())  # dim : Ns * m
    system_log.debug((f'Dims of Localization 1 : {L1[1].shape}'))

    local = lol.Local_class(
        model_lon[map_lon_idx], model_lat[map_lat_idx], model_lon[map_lon_idx],
        model_lat[map_lat_idx],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'])
    L2.append(local.calculate())  # dim : m * m
    system_log.debug((f'Dims of Localization 2 : {L2[1].shape}'))

    system_log.debug('Localization took %.2f s' %
                     ((datetime.now() - start_local).total_seconds()))

# create a memory-shared variables to store results
shared_array = mp.Array('f', X_f_dust.size)
X_a_dust = np.frombuffer(shared_array.get_obj(),
                         dtype=np.float32).reshape(X_f_dust.shape)

shared_array = mp.Array('f', x_f_dust_mean.size)
x_a_dust = np.frombuffer(shared_array.get_obj(),
                         dtype=np.float32).reshape(x_f_dust_mean.shape)

# X_a_dust = np.zeros([Nlev, Ns, Ne])
# x_a_dust = np.zeros([Nlev, Ns, 1])


def posterior_updater(i_lev: int) -> None:

    start_cal = datetime.now()

    ### *--- gather the pertubation matrix ---* ###
    X_dust_pertubate = X_f_dust[i_lev, :, :] - x_f_dust_mean[i_lev, :, :]
    X_aod_pertubate = X_f_aod[i_lev, :, :] - x_f_aod_mean[i_lev, :, :]

    # on ground level
    if i_lev == 0:

        ### *--- gather the U matrix ---* ###
        # dim : m * N
        U_dust = X_dust_pertubate[obs.map_idx['bc_pm10'], :]
        U_modis = X_aod_pertubate[obs.map_idx['modis_dod'], :]
        U_viirs = X_aod_pertubate[obs.map_idx['viirs_dod'], :]
        U = np.concatenate((U_dust, U_modis, U_viirs), axis=0)

        ### *--- gather the observations ---* ###
        # dim : m * 1
        value_dust = obs.values['bc_pm10'].reshape(-1, 1)
        value_modis = obs.values['modis_dod'][:, i_lev].reshape(-1, 1)
        value_viirs = obs.values['viirs_dod'][:, i_lev].reshape(-1, 1)
        y = np.concatenate((value_dust, value_modis, value_viirs), axis=0)

        ### *--- gather the obervational error ---* ###
        # dim : m * 1
        error_dust = obs.error['bc_pm10'].reshape(-1, 1)
        error_modis = obs.error['modis_dod'][:, i_lev].reshape(-1, 1)
        error_viirs = obs.error['viirs_dod'][:, i_lev].reshape(-1, 1)
        obs_error = np.concatenate((error_dust, error_modis, error_viirs),
                                   axis=0)

        ### *--- gather the innovation ---* ###
        # dim : m * Ne
        innovation_ensemble = y - np.concatenate(
            (X_f_dust[i_lev, obs.map_idx['bc_pm10'], :],
             X_f_aod[i_lev, obs.map_idx['modis_dod'], :],
             X_f_aod[i_lev, obs.map_idx['viirs_dod'], :]))
        # dim : m * 1
        innovation_mean = y - np.concatenate(
            (x_f_dust_mean[i_lev, obs.map_idx['bc_pm10']],
             x_f_aod_mean[i_lev, obs.map_idx['modis_dod']],
             x_f_aod_mean[i_lev, obs.map_idx['viirs_dod']]))

    # on upper level
    else:

        U_modis = X_aod_pertubate[obs.map_idx['modis_dod'], :]
        U_viirs = X_aod_pertubate[obs.map_idx['viirs_dod'], :]
        U = np.concatenate((U_modis, U_viirs), axis=0)

        value_modis = obs.values['modis_dod'][:, i_lev].reshape(-1, 1)
        value_viirs = obs.values['viirs_dod'][:, i_lev].reshape(-1, 1)
        y = np.concatenate((value_modis, value_viirs), axis=0)

        error_modis = obs.error['modis_dod'][:, i_lev].reshape(-1, 1)
        error_viirs = obs.error['viirs_dod'][:, i_lev].reshape(-1, 1)
        obs_error = np.concatenate((error_modis, error_viirs), axis=0)

        innovation_ensemble = y - np.concatenate(
            (X_f_aod[i_lev, obs.map_idx['modis_dod'], :],
             X_f_aod[i_lev, obs.map_idx['viirs_dod'], :]))

        innovation_mean = y - np.concatenate(
            (x_f_aod_mean[i_lev, obs.map_idx['modis_dod']],
             x_f_aod_mean[i_lev, obs.map_idx['viirs_dod']]))

    ### *--- calculate the Kalman Gain ---* ###
    if Config['Assimilation'][assimilation_scheme]['use_localization']:
        if i_lev == 0:
            K = L1[0] * (X_dust_pertubate @ U.T) / (
                Ne - 1) @ inv(L2[0] * (U @ U.T) /
                              (Ne - 1) + np.diag((obs_error**2).reshape(-1)))
        else:
            K = L1[1] * (X_dust_pertubate @ U.T) / (
                Ne - 1) @ inv(L2[1] * (U @ U.T) /
                              (Ne - 1) + np.diag((obs_error**2).reshape(-1)))
    else:
        K = (X_dust_pertubate @ U.T /
             (Ne - 1)) @ inv((U @ U.T) /
                             (Ne - 1) + np.diag((obs_error**2).reshape(-1)))

    ### *--- calculate the ensemble posteriors ---* ###
    epsilon = np.random.normal(loc=0,
                               scale=obs_error,
                               size=(len(obs_error), Ne))

    X_a_dust[i_lev, :, :] = X_f_dust[i_lev, :, :] + K @ (innovation_ensemble +
                                                         epsilon)
    x_a_dust[i_lev, :, :] = x_f_dust_mean[i_lev, :] + K @ innovation_mean

    system_log.debug('Posteriors on %sth layer finished, took %.2f s' %
                     (i_lev, ((datetime.now() - start_cal).total_seconds())))


### *--- enable multiprocessing to calcualate the ensemble posteriors ---* ###
with mp.Pool(processes=2) as pool:
    args_list = np.arange(Nlev - 3)
    results = pool.map_async(posterior_updater, args_list, chunksize=1)
    results.wait()
    pool.close()
    pool.join()
# for i_lev in range(Nlev):
#     posterior_updater(i_lev)

X_a_dust[np.logical_or(np.isnan(X_a_dust), X_a_dust <= 1e-9)] = 0
x_a_dust[np.logical_or(np.isnan(x_a_dust), x_a_dust <= 1e-9)] = 0
# for i_lev in range(Nlev - 3):
#     system_log.debug(np.mean(x_f_dust_mean[i_lev, :]))
#     system_log.debug(np.mean(x_a_dust[i_lev, :]))
#     system_log.debug(np.mean(X_a_dust[i_lev, :, :]))

### *--------------------------------------------* ###
### *--- Section 5 : Post-process the results ---* ###
### *--------------------------------------------* ###

### *--- write back to the Model restart files ---* ###
if Config['Assimilation'][assimilation_scheme]['write_restart']:

    ### *--- write back to model restart files ---* ###
    wr = lewl.Write_Restart(
        Config['Model'][model_scheme]['path']['model_output_path'],
        Config['Model'][model_scheme]['run_project'])

    for i_ensem in range(Ne):

        run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
        X_a_dust_write = X_a_dust[:, :, i_ensem].reshape([Nlev, Nlat, Nlon
                                                          ]) * dust_ratio
        wr.write(X_a_dust_write, run_id, assimilation_time, 'c')

    system_log.info('Ensemble posteriors have been written.')

### *--- save the posterior ---* ###
if Config['Assimilation']['post_process']['save_variables']:

    x_a_dust_sfc = x_a_dust[0, :].reshape([Nlat, Nlon])
    if Config['Model'][model_scheme]['run_type'] == 'ensemble':

        # save to netcdf format
        if Config['Assimilation']['post_process']['save_method'] == 'nc':
            var_output.save('posterior_dust_sfc', x_a_dust_sfc)

### *--------------------------------------* ###
### *--- tell main branch i am finished ---* ###
system_log.info('Assimilation finished, took %.2f s.' %
                ((datetime.now() - timer0).total_seconds()))
stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 100}})
