'''
Autor: Mijie Pang
Date: 2024-01-05 21:36:21
LastEditTime: 2024-01-10 16:04:12
Description: 
'''
import os
import sys
import argparse
import numpy as np
from numpy.linalg import inv
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

parser = argparse.ArgumentParser()
parser.add_argument('-assimilation_time',
                    default=Status['assimilation']['assimilation_time'])
args = parser.parse_args()

assimilation_time = datetime.strptime(args.assimilation_time,
                                      '%Y-%m-%d %H:%M:%S')

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
system_log.info('Working directory : %s' % (home_dir))

####################################################
###                                              ###
###              start Assimilation              ###
###                                              ###
####################################################

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
    model_output_dir = os.path.join(
        Config['Info']['path']['output_path'],
        Config['Model'][model_scheme]['run_project'], 'model_run', run_id,
        'output')

    X_f_aod_read[:, :, i_ensem] = mr.read_output(
        'aod2',
        'aod_550nm',
        assimilation_time,
        run_project=Config['Model'][model_scheme]['run_project'],
        run_id=run_id,
        output_dir=model_output_dir)

    for i_spec in range(len(dust_specs)):
        X_f_dust_read[i_spec, :, :, :, i_ensem] = mr.read_output(
            'conc-3d',
            dust_specs[i_spec],
            assimilation_time,
            run_project=Config['Model'][model_scheme]['run_project'],
            run_id=run_id,
            output_dir=model_output_dir,
            factor=1e9)

system_log.info('%s ensemble aod priors received.' % (Ne))
system_log.info('%s ensemble dust priors received.' % (Ne))

### *--- prepare the variables ---* ###
x_f_dust_mean = np.mean(X_f_dust_read, axis=-1)
# dust ratio in vertical, dim : Nlev * Ns
x_f_layers = np.sum(x_f_dust_mean, axis=0)
dust_ratio_layers = (x_f_layers / np.sum(x_f_layers, axis=0)).reshape(
    [Nlev, Ns])
dust_ratio_layers[np.isnan(dust_ratio_layers)] = 0
system_log.debug(
    f'layers vertical fraction : {np.mean(dust_ratio_layers,axis=-1)}')

# dust ratio for storing , dim : Nspec * Nlev * Ns
dust_ratio_sfc2layers = (x_f_dust_mean /
                         np.sum(x_f_dust_mean, axis=0)[0, :, :]).reshape(
                             [Nspec, Nlev, Ns])
dust_ratio_sfc2layers[np.isnan(dust_ratio_sfc2layers)] = 0
dust_ratio_sfc2layers[dust_ratio_sfc2layers > 9] = 9
system_log.debug(
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

### *--- MODIS DOD observation data ---* ###
obs.get_data('modis_dod', assimilation_time,
             **Config['Observation']['modis_dod'])
obs.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
obs.get_error('fraction', threshold=0.1, factor=0.3)

### *--- VIIRS DOD observation data ---* ###
obs.get_data('viirs_dod', assimilation_time,
             **Config['Observation']['viirs_dod'])
obs.map2obs('nearest', model_lon=model_lon, model_lat=model_lat)
obs.get_error('fraction', threshold=0.1, factor=0.3)

### *------------------------------------------* ###
### *---  Section 4 : calculate Posteriors  ---* ###
### *------------------------------------------* ###

### *-- localization ---* ###
if Config['Assimilation'][assimilation_scheme].get('use_localization', False):

    start_local = datetime.now()
    system_log.info(
        'Localization enabled, distance threshold : %s km' %
        (Config['Assimilation'][assimilation_scheme]['distance_threshold']))

    model_lon_meshed, model_lat_meshed = np.meshgrid(model_lon, model_lat)
    model_lon_meshed = np.ravel(model_lon_meshed)
    model_lat_meshed = np.ravel(model_lat_meshed)

    L_dict = {'type1': [], 'type2': []}  # ground and AOD

    ### *--- for the ground observations ---* ###
    local = lol.Local_class(
        model_lon_meshed, model_lat_meshed,
        model_lon_meshed[obs.map_idx['bc_pm10']],
        model_lat_meshed[obs.map_idx['bc_pm10']],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'])
    L_dict['type1'].append(local.calculate())  # dim : Ns * m

    local = lol.Local_class(
        model_lon_meshed[obs.map_idx['bc_pm10']],
        model_lat_meshed[obs.map_idx['bc_pm10']],
        model_lon_meshed[obs.map_idx['bc_pm10']],
        model_lat_meshed[obs.map_idx['bc_pm10']],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'])
    L_dict['type1'].append(local.calculate())  # dim : m * m
    system_log.debug(
        f'Dims of Localization type 1 : {L_dict["type1"][0].shape} and {L_dict["type1"][1].shape}'
    )

    ### *--- for the AOD observations ---* ###
    # local = lol.Local_class(
    #     model_lon_meshed, model_lat_meshed,
    #     np.concatenate((model_lon_meshed[obs.map_idx['modis_dod']],
    #                     model_lon_meshed[obs.map_idx['viirs_dod']])),
    #     np.concatenate((model_lat_meshed[obs.map_idx['modis_dod']],
    #                     model_lat_meshed[obs.map_idx['viirs_dod']])),
    #     Config['Assimilation'][assimilation_scheme]['distance_threshold'])
    # L_dict['type2'].append(local.calculate())  # dim : Ns * m

    # local = lol.Local_class(
    #     np.concatenate((model_lon_meshed[obs.map_idx['modis_dod']],
    #                     model_lon_meshed[obs.map_idx['viirs_dod']])),
    #     np.concatenate((model_lat_meshed[obs.map_idx['modis_dod']],
    #                     model_lat_meshed[obs.map_idx['viirs_dod']])),
    #     np.concatenate((model_lon_meshed[obs.map_idx['modis_dod']],
    #                     model_lon_meshed[obs.map_idx['viirs_dod']])),
    #     np.concatenate((model_lat_meshed[obs.map_idx['modis_dod']],
    #                     model_lat_meshed[obs.map_idx['viirs_dod']])),
    #     Config['Assimilation'][assimilation_scheme]['distance_threshold'])
    # L_dict['type2'].append(local.calculate())  # dim : m * m
    # system_log.debug(
    #     f'Dims of Localization type 2 : {L_dict["type2"][0].shape} and {L_dict["type2"][1].shape}'
    # )
    system_log.info('Localization took %.2f s' %
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
U_modis = X_aod_pertubate[obs.map_idx['modis_dod'], :].reshape(-1, Ne)
U_viirs = X_aod_pertubate[obs.map_idx['viirs_dod'], :].reshape(-1, Ne)
U = np.concatenate((U_modis, U_viirs), axis=0)

### *--- gather the observations ---* ###
# dim : m * 1
value_modis = obs.values['modis_dod'].reshape(-1, 1)
value_viirs = obs.values['viirs_dod'].reshape(-1, 1)
y = np.concatenate((value_modis, value_viirs), axis=0)

### *--- gather the obervational error ---* ###
# dim : m * 1
error_modis = obs.error['modis_dod'].reshape(-1, 1)
error_viirs = obs.error['viirs_dod'].reshape(-1, 1)
obs_error = np.concatenate((error_modis, error_viirs), axis=0)

### *--- gather the innovation ---* ###
# dim : m * 1
innovation_ensemble = y - np.concatenate(
    (X_f_aod[obs.map_idx['modis_dod'], :],
     X_f_aod[obs.map_idx['viirs_dod'], :]))
innovation_mean = y - np.concatenate((x_f_aod_mean[obs.map_idx['modis_dod']],
                                      x_f_aod_mean[obs.map_idx['viirs_dod']]))

### *--- calculate the Kalman Gain and Posterior ---* ###
R = np.diag((obs_error**2).reshape(-1))
# if Config['Assimilation'][assimilation_scheme].get('use_localization', False):
#     K = L_dict['type2'][0] * (X_dust_pertubate_all_layers @ U.T) / (
#         Ne - 1) @ inv(L_dict['type2'][1] * (U @ U.T) / (Ne - 1) + R)
# else:
K = (X_dust_pertubate_all_layers @ U.T / (Ne - 1)) @ inv((U @ U.T) /
                                                         (Ne - 1) + R)

epsilon = np.random.normal(loc=0, scale=obs_error, size=(len(obs_error), Ne))

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
U_dust = X_dust_sfc_pertubate[obs.map_idx['bc_pm10'], :].reshape(-1, Ne)
U = U_dust

### *--- gather the observations ---* ###
# dim : m * 1
value_dust = obs.values['bc_pm10'].reshape(-1, 1)
y = value_dust

### *--- gather the obervational error ---* ###
# dim : m * 1
error_dust = obs.error['bc_pm10'].reshape(-1, 1)
obs_error = error_dust

### *--- gather the innovation ---* ###
# dim : m * 1
innovation_mean = y - x_f_dust_sfc[obs.map_idx['bc_pm10']]

### *--- calculate the Kalman Gain and Posterior ---* ###
R = np.diag((obs_error**2).reshape(-1))
if Config['Assimilation'][assimilation_scheme].get('use_localization', False):
    K = L_dict['type1'][0] * (X_dust_pertubate_all_layers @ U.T) / (
        Ne - 1) @ inv(L_dict['type1'][1] * (U @ U.T) / (Ne - 1) + R)
else:
    K = (X_dust_sfc_pertubate @ U.T / (Ne - 1)) @ inv((U @ U.T) / (Ne - 1) + R)

x_a_dust_sfc = (x_f_dust_sfc + K @ innovation_mean).astype(float)

system_log.info('Posteriors calculation finished, took %.2f s' %
                ((datetime.now() - start_cal).total_seconds()))

### *--- restore the dust full structure ---* ###
x_a_dust = x_a_dust_sfc.reshape(-1) * dust_ratio_sfc2layers
x_a_dust[np.logical_or(np.isnan(x_a_dust), x_a_dust <= 1e-9)] = 0
system_log.info('Mean of posterior : %.2f and Mean of prior : %.2f' %
                (np.mean(np.sum(x_a_dust, axis=0)), np.mean(x_f_dust_mean)))

### *--------------------------------------------* ###
### *--- Section 5 : Post-process the results ---* ###
### *--------------------------------------------* ###

### *--- save the posterior ---* ###
if Config['Assimilation']['post_process']['save_variables']:

    if Config['Model'][model_scheme]['run_type'] == 'ensemble':

        # save to netCDF format
        if Config['Assimilation']['post_process']['save_method'] == 'nc':
            var_output.save('posterior_dust_3d', np.sum(x_a_dust, axis=0), alt)

### *--------------------------------------* ###
### *--- tell main branch i am finished ---* ###
system_log.info('Assimilation finished, took %.2f s.' %
                ((datetime.now() - timer0).total_seconds()))
stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 100}})
