'''
Autor: Mijie Pang
Date: 2023-12-25 15:57:41
LastEditTime: 2024-01-05 14:20:57
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

dust_specs = ['dust_ff', 'dust_f', 'dust_c', 'dust_cc', 'dust_ccc']

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

    var_output = leopl.Output(save_variables_dir,
                              Config['Model'][model_scheme], **Config)

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

### *-------------------------------------------------* ###
### *--- Section 2 : retrive ensemble model priors ---* ###
### *-------------------------------------------------* ###

### *--- initialize the variables ---* ###
X_f_aod_read = np.zeros([Nlat, Nlon, Ne])  # aod priori matrix
X_f_dust_read = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne])  # dust priori matrix

### *--- read the model results ---* ###
mr = lerl.Model_Reader(
    Config['Model'][model_scheme]['path']['model_output_path'])
alt = mr.read_output('conc-3d',
                     'altitude',
                     assimilation_time,
                     run_project=Config['Model'][model_scheme]['run_project'],
                     run_id='iter_00_ensem_00',
                     output_dir=os.path.join(
                         Config['Info']['path']['output_path'],
                         Config['Model'][model_scheme]['run_project'],
                         'model_run', 'iter_00_ensem_00', 'output'))

for i_ensem in range(Ne):

    run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
    X_f_aod_read[:, :, i_ensem] = mr.read_output(
        'aod2',
        'aod_550nm',
        assimilation_time,
        run_project=Config['Model'][model_scheme]['run_project'],
        run_id=run_id,
        output_dir=os.path.join(Config['Info']['path']['output_path'],
                                Config['Model'][model_scheme]['run_project'],
                                'model_run', run_id, 'output'))

    for i_spec in range(len(dust_specs)):
        X_f_dust_read[i_spec, :, :, :, i_ensem] = mr.read_output(
            'conc-3d',
            dust_specs[i_spec],
            assimilation_time,
            run_project=Config['Model'][model_scheme]['run_project'],
            run_id=run_id,
            output_dir=os.path.join(
                Config['Info']['path']['output_path'],
                Config['Model'][model_scheme]['run_project'], 'model_run',
                run_id, 'output'),
            factor=1e9)

system_log.info('%s ensemble aod priors received.' % (Ne))
system_log.info('%s ensemble dust priors received.' % (Ne))

### *--- prepare the variables ---* ###
# ensemble dust and mean
X_f_dust = np.sum(X_f_dust_read, axis=0).reshape(Nlev, Ns, Ne)
x_f_dust_mean = np.mean(X_f_dust, axis=-1).reshape([Nlev, Ns, 1])

# dust ratio
dust_ratio = np.zeros([Nspec, Nlev, Nlat, Nlon])
X_f_dust_mean = np.mean(X_f_dust_read, axis=-1)
# dim : Nspec * Nlev * Nlat * Nlon
dust_ratio = X_f_dust_mean / np.sum(X_f_dust_mean, axis=(0, 1), keepdims=True)
dust_ratio[np.isnan(dust_ratio)] = 0
# dust ratio in vertical, dim : Nlev * Ns
dust_ratio_layers = np.sum(dust_ratio, axis=0).reshape([Nlev, Ns])

# aod prior, dim : Nlev, Ns, Ne
X_f_aod = np.zeros([Nlev, Ns, Ne])
for i_ensem in range(Ne):
    X_f_aod[:, :, i_ensem] = (X_f_aod_read[:, :, i_ensem].reshape([1, Ns]) *
                              dust_ratio_layers)
X_f_aod[np.isnan(X_f_aod)] = 0
x_f_aod_mean = np.mean(X_f_aod, axis=-1).reshape([Nlev, Ns, 1])

### *-- determine the localization in model space ---* ###
local_thres = 1
local_rate = 100
while local_rate > 25:
    local_bools = x_f_dust_mean > local_thres
    local_rate = np.sum(local_bools.astype(int)) / local_bools.size

local_bools = local_bools.reshape([Nlev, Ns])
system_log.info('Model localization filter rate : %.2f ' % (local_rate * 100) +
                '%')

# location space localization
lon_meshed, lat_meshed = np.meshgrid(model_lon, model_lat)
lon_space = np.array([lon_meshed for i_lev in range(Nlev)]).reshape([Nlev, Ns])
lat_space = np.array([lat_meshed for i_lev in range(Nlev)]).reshape([Nlev, Ns])
lon_space_local = lon_space[local_bools]
lat_space_local = lat_space[local_bools]

# state space localization
x_f_dust_local = x_f_dust_mean[local_bools].reshape(-1, 1)
x_f_aod_local = x_f_aod_mean[local_bools].reshape(-1, 1)

X_f_dust_local = np.zeros([x_f_dust_local.size, Ne])
X_f_aod_local = np.zeros([x_f_aod_local.size, Ne])
for i_ensem in range(Ne):
    X_f_dust_local[:, i_ensem] = X_f_dust[local_bools, i_ensem]
    X_f_aod_local[:, i_ensem] = X_f_aod[local_bools, i_ensem]

### *--- save the ensemble prior ---* ###
if Config['Assimilation']['post_process']['save_variables']:

    # save to netCDF format
    if Config['Assimilation']['post_process']['save_method'] == 'nc':
        var_output.save('prior_aod', np.mean(X_f_aod_read, axis=-1))
        var_output.save('prior_dust_3d',
                        x_f_dust_mean.reshape([Nlev, Nlat, Nlon]), alt)

### *---------------------------------------------------* ###
### *---        Section 3 : read observations        ---* ###
### *---------------------------------------------------* ###

obs = leol.Observations(Config['Observation']['path'])

### *--- BC-PM10 observation data ---* ###
obs.get_data('bc_pm10', assimilation_time, **Config['Observation']['bc_pm10'])
obs.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
obs.get_error('fraction', threshold=200, factor=0.1)
obs.local_filter(local_bools[0, :])
system_log.debug('%s BC_PM10 in model space.' % (obs.values['bc_pm10'].size))

### *--- MODIS DOD observation data ---* ###
obs.get_data('modis_dod', assimilation_time,
             **Config['Observation']['modis_dod'])
obs.map2obs('nearest', model_lon=model_lon, model_lat=model_lat, repeat=Nlev)
obs.get_error('fraction', threshold=0.1, factor=0.3)
obs.layering(dust_ratio_layers)
obs.local_filter(local_bools)

if not obs.m['modis_dod'] == 0:
    for i_lev in range(Nlev - 8):
        obs.map_idx['modis_dod'][i_lev + 8] = np.empty(0, dtype=int)
        obs.values['modis_dod'][i_lev + 8] = np.empty(0)
        obs.error['modis_dod'][i_lev + 8] = np.empty(0)
# for i_lev in range(Nlev):
#     system_log.info('%s %s' % (np.mean(obs.values['modis_dod'][i_lev]),
#                                np.mean(obs.error['modis_dod'][i_lev])))

obs.reduce_dim(Ns, Nlev)
system_log.debug('%s MODIS DOD in model space.' %
                 (obs.values['modis_dod'].size))

### *--- VIIRS DOD observation data ---* ###
obs.get_data('viirs_dod', assimilation_time,
             **Config['Observation']['viirs_dod'])
obs.map2obs('nearest', model_lon=model_lon, model_lat=model_lat, repeat=Nlev)
obs.get_error('fraction', threshold=0.1, factor=0.3)
obs.layering(dust_ratio_layers)
obs.local_filter(local_bools)

if not obs.m['viirs_dod'] == 0:
    for i_lev in range(Nlev - 8):
        obs.map_idx['viirs_dod'][i_lev + 8] = np.empty(0, dtype=int)
        obs.values['viirs_dod'][i_lev + 8] = np.empty(0)
        obs.error['viirs_dod'][i_lev + 8] = np.empty(0)

obs.reduce_dim(Ns, Nlev)
system_log.debug('%s VIIRS DOD in model space.' %
                 (obs.values['viirs_dod'].size))

### *------------------------------------------* ###
### *---  Section 4 : calculate Posteriors  ---* ###
### *------------------------------------------* ###

start_cal = datetime.now()

### *--- gather the pertubation matrix ---* ###
# dim : n * N
X_dust_pertubate = (X_f_dust - x_f_dust_mean).reshape([Nlev * Ns, Ne])
X_aod_pertubate = (X_f_aod - x_f_aod_mean).reshape([Nlev * Ns, Ne])
X_dust_pertubate_local = X_f_dust_local - x_f_dust_local
X_aod_pertubate_local = X_f_aod_local - x_f_aod_local

### *--- gather the U matrix ---* ###
# dim : m * N
U_dust = X_dust_pertubate[obs.map_idx['bc_pm10'], :].reshape(-1, Ne)
U_modis = X_aod_pertubate[obs.map_idx['modis_dod'], :].reshape(-1, Ne)
U_viirs = X_aod_pertubate[obs.map_idx['viirs_dod'], :].reshape(-1, Ne)
U = np.concatenate((U_dust, U_modis, U_viirs), axis=0)

### *--- gather the observations ---* ###
# dim : m * 1
value_dust = obs.values['bc_pm10'].reshape(-1, 1)
value_modis = obs.values['modis_dod'].reshape(-1, 1)
value_viirs = obs.values['viirs_dod'].reshape(-1, 1)
y = np.concatenate((value_dust, value_modis, value_viirs), axis=0)

### *--- gather the obervational error ---* ###
# dim : m * 1
error_dust = obs.error['bc_pm10'].reshape(-1, 1)
error_modis = obs.error['modis_dod'].reshape(-1, 1)
error_viirs = obs.error['viirs_dod'].reshape(-1, 1)
obs_error = np.concatenate((error_dust, error_modis, error_viirs), axis=0)

### *--- gather the innovation ---* ###
# dim : m * 1
x_f_dust_mean_tmp = x_f_dust_mean.reshape([Nlev * Ns, 1])
x_f_aod_mean_tmp = x_f_aod_mean.reshape([Nlev * Ns, 1])
innovation_mean = y - np.concatenate(
    (x_f_dust_mean_tmp[obs.map_idx['bc_pm10']],
     x_f_aod_mean_tmp[obs.map_idx['modis_dod']],
     x_f_aod_mean_tmp[obs.map_idx['viirs_dod']]))

### *--- calculate the Kalman Gain and Posterior ---* ###
K = (X_dust_pertubate_local @ U.T /
     (Ne - 1)) @ inv((U @ U.T) /
                     (Ne - 1) + np.diag((obs_error**2).reshape(-1)))

x_a_dust_local = x_f_dust_local + K @ innovation_mean

system_log.info('Posteriors calculation finished, took %.2f s' %
                ((datetime.now() - start_cal).total_seconds()))

### *--- restore the dust full structure ---* ###
x_a_dust = np.zeros(x_f_dust_mean.shape)
x_a_dust[local_bools] = x_a_dust_local
x_a_dust[np.logical_or(np.isnan(x_a_dust), x_a_dust <= 1e-9)] = 0
system_log.info('Mean of posterior : %s and Mean of prior : %s' %
                (np.mean(x_a_dust), np.mean(x_f_dust_mean)))

### *--------------------------------------------* ###
### *--- Section 5 : Post-process the results ---* ###
### *--------------------------------------------* ###

### *--- save the posterior ---* ###
if Config['Assimilation']['post_process']['save_variables']:

    if Config['Model'][model_scheme]['run_type'] == 'ensemble':

        # save to netCDF format
        if Config['Assimilation']['post_process']['save_method'] == 'nc':
            var_output.save('posterior_dust_3d', x_a_dust, alt)

### *--------------------------------------* ###
### *--- tell main branch i am finished ---* ###
system_log.info('Assimilation finished, took %.2f s.' %
                ((datetime.now() - timer0).total_seconds()))
stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 100}})
