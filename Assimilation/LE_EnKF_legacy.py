'''
Autor: Mijie Pang
Date: 2023-03-24 18:01:04
LastEditTime: 2023-12-09 20:14:06
Description: 
'''
import os
import sys
import numpy as np
import netCDF4 as nc
from datetime import datetime

import LE_asml_lib as leal
import LE_obs_lib as leol
import Assimilation_lib as asl

sys.path.append('../')
import system_lib as stl
from tool.pack import NcProduct

home_dir = os.getcwd()
config_dir = '../config'
status_path = '../Status.json'

################################################
###            read configuration            ###
Config = stl.read_json_dict(config_dir, 'Assimilation.json', 'Model.json',
                            'Observation.json', 'Info.json')
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']

### initialize the system log ###
system_log = stl.Logging(Status['system']['home_dir'],
                         **Config['Info']['System'])

### initial the parameters ###
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

### set the system status ###
timer0 = datetime.now()
stl.edit_json(path=status_path,
              new_dict={'assimilation': {
                  'code': 10,
                  'home_dir': home_dir
              }})

##############################################
###                                        ###
###           start assimilation           ###
###                                        ###
##############################################

### initialize the states ###
X_f = np.zeros([Ns, Ne])
X_f_3d = np.zeros([Ne, Nspec, Nlev, Nlat, Nlon])
spec_partition = np.zeros([Nlev, Nspec])
mass_partition = np.zeros([Nlev, Nlat, Nlon])
mass_partition[0, :, :] = Ne

################################################
###    retrive ensemble model simulations    ###
for i_ensemble in range(Ne):

    run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensemble)

    with nc.Dataset(
            os.path.join(
                Config['Model'][model_scheme]['path']['model_output_path'],
                Config['Model'][model_scheme]['run_project'], run_id,
                'restart', 'LE_%s_state_%s.nc' %
                (run_id,
                 assimilation_time.strftime('%Y%m%d_%H%M')))) as nc_obj:

        X_f_3d[i_ensemble, :] = nc_obj.variables['c'][:]

    ### convert 2D to 3D ###
    ## mass partition ##
    for level in range(Nlev - 1):
        mass_partition[level + 1, :, :] += np.sum(X_f_3d[i_ensemble,:, level + 1, :, :], axis=0) \
                                            / np.sum(X_f_3d[i_ensemble,:, 0, :, :], axis=0)

    ## 5 specs partition ##
    for level in range(Nlev):
        for sp in range(Nspec):
            spec_partition[level,sp] += (np.sum(X_f_3d[i_ensemble, sp, level, :, :])) / \
                                        (np.sum(X_f_3d[i_ensemble, :, level, :, :]))

    X_f[:,
        i_ensemble] = np.ravel(np.sum(X_f_3d[i_ensemble, :, 0, :, :], axis=0))

mass_partition = mass_partition / Ne
mass_partition[mass_partition > 99] = 0e-9
spec_partition = spec_partition / Ne
system_log.info('%s ensemble priors received' % (Ne))

### save the variables ###
x_f_mean = np.mean(np.sum(X_f_3d, axis=1), axis=0)[0, :, :]
x_f_3d = np.mean(np.sum(X_f_3d, axis=1), axis=0)
if Config['Assimilation']['post_process']['save_variables']:

    if Config['Assimilation']['post_process']['save_method'] == 'npy':

        asl.save2npy(dir_name=save_variables_dir,
                     variables={
                         'priori': x_f_mean,
                     })

    elif Config['Assimilation']['post_process']['save_method'] == 'nc':

        ### initiate the nc file ###
        nc_product = NcProduct(os.path.join(save_variables_dir, 'priori.nc'),
                               Model=Config['Model'],
                               Assimilation=Config['Assimilation'],
                               Info=Config['Info'])
        nc_product.define_dimension(longitude=Nlon, latitude=Nlat, level=Nlev)
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            level=['f4', 'level'],
            priori=['f4', ('level', 'latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=model_lon)
        nc_product.add_data(latitude=model_lat)
        nc_product.add_data(level=range(0, Nlev))
        nc_product.add_data(priori=x_f_3d.reshape(
            [Nlev, len(model_lat), len(model_lon)]))

        nc_product.close()

###################################################
###   calculate background covariance matrix    ###
x_f_mean = np.mean(X_f, axis=1)
X_pertubate = X_f - x_f_mean
P_f = np.cov(X_pertubate.T, ddof=1)

##############################################################
###    read  observations and map model space into them    ###
obs_data = leol.read_obs(obs_dir=os.path.join(
    Config['Observation']['path'], Config['Observation']['bc_pm10']['dir']),
                         time=assimilation_time,
                         screen=True,
                         model_lon=model_lon,
                         model_lat=model_lat)
m = len(obs_data)  # m is the number of valid observations
y = np.array(obs_data.iloc[:, 2]).reshape([m, 1])
H = np.zeros([m, Ns])
O = np.zeros([m, m])
sigma = np.zeros([m])

for i_obs in range(m):

    map_lon = asl.find_nearest(obs_data.iloc[i_obs, 0], model_lon)
    map_lat = asl.find_nearest(obs_data.iloc[i_obs, 1], model_lat)

    ### assign the H operator ###
    H_map_index = int(map_lat * Nlon + map_lon)
    H[i_obs, H_map_index] = 1

    ### assign the observational error matrix ###
    sigma[i_obs] = leol.assign_obs_error(y[i_obs])
    O[i_obs, i_obs] = sigma[i_obs]**2

system_log.info('%s observations received' % (m))

#############################################
###        covariance localization        ###
if Config['Assimilation'][assimilation_scheme]['use_localization']:

    start_local = datetime.now()
    system_log.debug(
        'Localization enabled, distance threshold : %s km' %
        (Config['Assimilation'][assimilation_scheme]['distance_threshold']))

    L_path = os.path.join(
        Config['Info']['path']['output_path'], 'localization', 'L_%s.npy' %
        (Config['Assimilation'][assimilation_scheme]['distance_threshold']))
    if not os.path.exists(L_path):
        L = leal.localization(
            model_lon, model_lat,
            Config['Assimilation'][assimilation_scheme]['distance_threshold'])
        np.save(L_path, L)

    L = np.load(L_path)

    P_f = np.multiply(L, P_f)

    system_log.debug('localization took %.2f s' %
                     ((datetime.now() - start_local).total_seconds()))

#########################################
###       calculate Kalman Gain       ###
K = asl.calculate_kalman_gain(P_f=P_f, H=H, R=O)
system_log.info('Kalman Gain calculated')

##########################################################
###  calculate the posteriori and write back to model  ###
if Config['Model'][model_scheme]['run_type'] == 'ensemble':

    ### calculate the posteriori ###
    X_a = np.zeros([Ne, Ns])
    X_a_3d = np.zeros([Ne, Nspec, Nlev, Nlat, Nlon])
    for i_ensem in range(Ne):

        y_i = np.zeros([m, 1])
        for j in range(m):
            y_i[j, 0] = y[j] + np.random.normal(loc=0, scale=sigma[j], size=1)

        x_f_i = X_f[:, i_ensem].reshape([Ns, 1])

        X_a[i_ensem, :] = asl.calculate_posteriori(x_f=x_f_i, K=K, y=y_i, H=H)
        X_a_3d[i_ensem, :] = leal.convert2full(posteriori_2d=X_a[i_ensem, :],
                                               Nspec=Nspec,
                                               nlon=Nlon,
                                               nlat=Nlat,
                                               nlevel=Nlev,
                                               mass_partition=mass_partition,
                                               spec_partition=spec_partition)

    ### write back to the model restart files ###
    if Config['Assimilation'][assimilation_scheme]['write_restart']:

        leal.write_new_nc_ensemble(
            posteriori=X_a_3d,
            Ne=Ne,
            iteration_num=iteration_num,
            time=assimilation_time,
            restart_path=os.path.join(
                Config['Model'][model_scheme]['path']['model_output_path'],
                Config['Model'][model_scheme]['run_project']))

######################################################
###     save the variables for post processing     ###
if Config['Assimilation']['post_process']['save_variables']:

    x_a_mean = asl.calculate_posteriori(x_f=x_f_mean.reshape([Ns, 1]),
                                        K=K,
                                        y=y,
                                        H=H).reshape([Nlat, Nlon])
    x_a_3d = np.sum(leal.convert2full(posteriori_2d=x_a_mean,
                                      Nspec=Nspec,
                                      nlon=Nlon,
                                      nlat=Nlat,
                                      nlevel=Nlev,
                                      mass_partition=mass_partition,
                                      spec_partition=spec_partition),
                    axis=0)

    if Config['Model'][model_scheme]['run_type'] == 'ensemble':

        if Config['Assimilation']['post_process']['save_method'] == 'npy':

            asl.save2npy(dir_name=save_variables_dir,
                         variables={
                             'posteriori': x_a_mean.reshape([Nlat, Nlon]),
                         })

        elif Config['Assimilation']['post_process']['save_method'] == 'nc':

            ### initiate the nc file ###
            nc_product = NcProduct(os.path.join(save_variables_dir,
                                                'posteriori.nc'),
                                   Model=Config['Model'],
                                   Assimilation=Config['Assimilation'],
                                   Info=Config['Info'])
            nc_product.define_dimension(longitude=Nlon,
                                        latitude=Nlat,
                                        level=Nlev)
            nc_product.define_variable(
                longitude=['f4', 'longitude'],
                latitude=['f4', 'latitude'],
                level=['f4', 'level'],
                posteriori=['f4', ('level', 'latitude', 'longitude')])

            ### add data to the file ###
            nc_product.add_data(longitude=model_lon)
            nc_product.add_data(latitude=model_lat)
            nc_product.add_data(level=range(0, Nlev))
            nc_product.add_data(posteriori=x_a_3d.reshape(
                [Nlev, len(model_lat), len(model_lon)]))

            nc_product.close()

##########################################
###   tell main branch I am finished   ###
system_log.info('Assimilation finished, took {:.2f} s'.format(
    (datetime.now() - timer0).total_seconds()))
stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 100}})
