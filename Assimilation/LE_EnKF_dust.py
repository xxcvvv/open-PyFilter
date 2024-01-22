'''
Autor: Mijie Pang
Date: 2023-02-16 16:37:57
LastEditTime: 2023-12-17 15:15:55
Description: 
'''
import os
import sys
import numpy as np
from numpy.linalg import inv
from datetime import datetime

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

#####################################################
###     Section 1 : initialize configurations     ###
Config = stl.read_json_dict(config_dir, 'Assimilation.json', 'Model.json',
                            'Observation.json', 'Info.json')
Status = stl.read_json(path=status_path)

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

### set the system status ###
timer0 = datetime.now()
stl.edit_json(path=status_path,
              new_dict={'assimilation': {
                  'code': 10,
                  'home_dir': home_dir
              }})

### debug information ###
system_log.debug('working directory : %s' % (home_dir))

####################################################
###                                              ###
###              start Assimilation              ###
###                                              ###
####################################################

### initialize the variables ###
X_f = np.zeros([Ns, Ne])  # priori matrix
X_f_3d = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne])  # 3d priori matrix
spec_partition = np.zeros([Nlev, Nspec])  # species partition
mass_partition = np.zeros([Nlev, Nlat, Nlon])  # mass partition
mass_partition[0, :, :] = Ne

#######################################################
###    Section 2 : retrive ensemble model priors    ###
mr = lerl.Model_Reader(
    Config['Model'][model_scheme]['path']['model_output_path'])
for i_ensem in range(Ne):

    run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
    X_f_3d[:, :, :, :, i_ensem] = mr.read_restart(
        Config['Model'][model_scheme]['run_project'], run_id,
        assimilation_time)

    ### convert 2D to 3D ###
    # mass partition #
    for level in range(Nlev - 1):
        mass_partition[level + 1, :, :] += np.sum(
            X_f_3d[:, level + 1, :, :, i_ensem], axis=0) / np.sum(
                X_f_3d[:, 0, :, :, i_ensem], axis=0)

    # 5 species partition #
    for level in range(Nlev):
        for sp in range(Nspec):
            spec_partition[level, sp] += np.sum(
                X_f_3d[sp, level, :, :, i_ensem]) / np.sum(
                    X_f_3d[:, level, :, :, i_ensem])

mass_partition = mass_partition / Ne
mass_partition[mass_partition > 99] = 1
spec_partition = spec_partition / Ne

system_log.info('%s ensemble priors received' % (Ne))

X_f = np.sum(X_f_3d[:, 0, :, :, :],
             axis=0).reshape(Ns, Ne)  # ensemble ground field, dim : Ns * Ne
x_f_mean = np.mean(X_f, axis=1)  # mean of priori, dim : Ns
x_f_3d = np.mean(np.sum(X_f_3d, axis=0),
                 axis=3)  # 3d priori, dim : Nlev * Nlat * Nlon

### save the ensemble priori ###
if Config['Assimilation']['post_process']['save_variables']:

    ### save to numpy format ###
    if Config['Assimilation']['post_process']['save_method'] == 'npy':
        asl.save2npy(dir_name=save_variables_dir,
                     variables={
                         'priori': x_f_mean.reshape([Nlat, Nlon]),
                     })

    ### save to netcdf format ###
    elif Config['Assimilation']['post_process']['save_method'] == 'nc':
        var_output.save('prior_dust_3d', x_f_3d)

#################################################
###       Section 3 : read observations       ###

obs = leol.Observation(Config['Observation']['path'], 'bc_pm10')
obs.get_data(assimilation_time, Config['Observation']['bc_pm10']['dir_name'])
obs.map2obs('nearest', model_lon, model_lat)
obs.get_error('fraction', threshold=200, factor=0.1)

system_log.info('%s observations received' % (obs.m))

##################################################
###     Section 4 : calculate Kalman Gain      ###

X_pertubate = X_f - x_f_mean.reshape([Ns, 1])
U = obs.H @ X_pertubate

### localization ###
if Config['Assimilation'][assimilation_scheme]['use_localization']:

    start_local = datetime.now()
    system_log.debug(
        'Localization enabled, distance threshold : %s km' %
        (Config['Assimilation'][assimilation_scheme]['distance_threshold']))

    local = lol.Local_class(
        model_lon,
        model_lat,
        model_lon[obs.map_lon_idx],
        model_lat[obs.map_lat_idx],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'],
        meshgrid1=True)
    L1 = local.calculate()  # dim : Ns * m
    system_log.debug((f'Dims of Localization 1 : {L1.shape}'))

    local = lol.Local_class(
        model_lon[obs.map_lon_idx], model_lat[obs.map_lat_idx],
        model_lon[obs.map_lon_idx], model_lat[obs.map_lat_idx],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'])
    L2 = local.calculate()  # dim : m * m
    system_log.debug((f'Dims of Localization 2 : {L2.shape}'))

    system_log.debug('localization took %.2f s' %
                     ((datetime.now() - start_local).total_seconds()))

    K = L1 * (X_pertubate @ U.T) / (Ne - 1) @ inv(L2 * (U @ U.T) /
                                                  (Ne - 1) + obs.O)

else:

    K = (X_pertubate @ U.T / (Ne - 1)) @ inv((U @ U.T) / (Ne - 1) + obs.O)

system_log.info('Kalman Gain calculated')

################################################
###     Section 5: update the posteriors     ###

if Config['Model'][model_scheme]['run_type'] == 'ensemble':

    ### calculate the posteriori ###
    X_a = np.zeros([Ns, Ne])
    X_a_3d = np.zeros([Nspec, Nlev, Nlat, Nlon, Ne])
    wr = lewl.Write_Restart(
        Config['Model'][model_scheme]['path']['model_output_path'],
        Config['Model'][model_scheme]['run_project'])

    for i_ensem in range(Ne):

        y_i = obs.values + np.random.normal(loc=0, scale=obs.error,
                                            size=obs.m).reshape([obs.m, 1])

        y_i = y_i.reshape([obs.m, 1])
        x_f_i = X_f[:, i_ensem].reshape([Ns, 1])

        X_a[:, i_ensem] = asl.calculate_posteriori(x_f=x_f_i,
                                                   K=K,
                                                   y=y_i,
                                                   H=obs.H)
        X_a_3d[:, :, :, :, i_ensem] = wr.convert2full(X_a[:, i_ensem],
                                                      mass_partition,
                                                      spec_partition,
                                                      Nspec=Nspec,
                                                      Nlon=Nlon,
                                                      Nlat=Nlat,
                                                      Nlev=Nlev)

    ### write back to the Model restart files ###
    if Config['Assimilation'][assimilation_scheme]['write_restart']:

        for i_ensem in range(Ne):
            run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
            wr.write(X_a_3d[:, :, :, :, i_ensem], run_id, assimilation_time,
                     'c')

##############################################
###     Section 6 : save the variables     ###
if Config['Assimilation']['post_process']['save_variables']:

    x_a_mean = asl.calculate_posteriori(x_f=x_f_mean.reshape([Ns, 1]),
                                        K=K,
                                        y=obs.values.reshape([obs.m, 1]),
                                        H=obs.H).reshape([Nlat, Nlon])

    x_a_3d = np.sum(wr.convert2full(
        x_a_mean,
        mass_partition,
        spec_partition,
        Nlon=Nlon,
        Nlat=Nlat,
    ),
                    axis=0)
    # system_log.debug(f'dims of x_a_3d : {x_a_3d.shape}')

    if Config['Model'][model_scheme]['run_type'] == 'ensemble':

        ### save to numpy format ###
        if Config['Assimilation']['post_process']['save_method'] == 'npy':

            asl.save2npy(dir_name=save_variables_dir,
                         variables={
                             'posteriori': x_a_mean.reshape([Nlat, Nlon]),
                         })

        ### save to netcdf format ###
        elif Config['Assimilation']['post_process']['save_method'] == 'nc':
            var_output.save('posterior_dust_3d', x_a_3d)

##########################################
###   tell main branch i am finished   ###
system_log.info('Assimilation finished, took %.2f s' %
                ((datetime.now() - timer0).total_seconds()))
stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 100}})
