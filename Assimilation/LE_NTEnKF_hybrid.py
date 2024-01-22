'''
Autor: Mijie Pang
Date: 2023-02-14 20:55:20
LastEditTime: 2023-11-03 10:27:08
Description: 
'''
import os
import sys
import numpy as np
from numpy.linalg import inv
import netCDF4 as nc
import multiprocessing as mp
from datetime import datetime, timedelta

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

assimilation_scheme = Config['Assimilation']['scheme']['name']
model_scheme = Config['Model']['scheme']['name']

### initialize the system log ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### initial the parameters ###
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

### set the system status ###
project_start_run_time = datetime.now()
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

### configure the time and ensemble set ###
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

system_log.debug(f'list of run id to read : {run_id_read}')
system_log.debug(f'list of time : {time_set}')
system_log.debug(f'list of ensemble set : {ensemble_set}')

### initiate the variables ###
X_f_3d = np.zeros([Ne_extend, Nspec, Nlev, Nlat, Nlon])
spec_partition = np.zeros([len(time_set), Nlev, Nspec])
mass_partition = np.zeros([len(time_set), Nlev, Nlat, Nlon])
mass_partition[:, 0, :, :] = Ne

##########################################
###   retrive ensemble model results   ###
for i_time in range(len(time_set)):
    for i_ensem in range(ensemble_set[i_time]):

        Ne_count = sum(ensemble_set[:i_time]) + i_ensem

        path = os.path.join(
            Config['Model'][model_scheme]['path']['model_output_path'],
            Config['Model'][model_scheme]['run_project'],
            run_id_read[i_time][i_ensem], 'restart',
            'LE_%s_state_%s.nc' % (run_id_read[i_time][i_ensem],
                                   time_set[i_time].strftime('%Y%m%d_%H%M')))

        with nc.Dataset(path) as nc_obj:
            X_f_3d[Ne_count, :] = nc_obj.variables['c'][:]

        ### convert 2D to 3D ###
        ## mass partition ##
        for level in range(Nlev - 1):
            mass_partition[i_time,level + 1, :, :] += np.sum(X_f_3d[Ne_count,:, level + 1, :, :], axis=0) / \
                                                        np.sum(X_f_3d[Ne_count,:, 0, :, :], axis=0)

        ## 5 specs partition ##
        for level in range(Nlev):
            for spec in range(Nspec):
                spec_partition[i_time, level, spec] += (np.sum(X_f_3d[Ne_count, spec, level, :, :])) / \
                                                        (np.sum(X_f_3d[Ne_count, :, level, :, :]))

### calculate average vertical structure ###
for i_time in range(len(time_set)):

    mass_partition[i_time] = mass_partition[i_time] / ensemble_set[i_time]
    mass_partition[mass_partition > 99] = 0e-9
    spec_partition[i_time] = spec_partition[i_time] / ensemble_set[i_time]

if Config['Assimilation']['post_process']['save_variables']:

    x_f_3d = np.mean(np.sum(X_f_3d, axis=1), axis=0)

    if Config['Assimilation']['post_process']['save_method'] == 'npy':

        asl.save2npy(dir_name=save_variables_dir,
                     variables={
                         'priori': np.sum(np.mean(X_f_3d, axis=0), axis=0),
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

system_log.info('%s ensemble priors received' % (Ne_extend))

##############################################################
###    read  observations and map model space into them    ###
obs_data = leol.read_obs(obs_dir=os.path.join(
    Config['Observation']['path'], Config['Observation']['bc_pm10']['dir']),
                         time=assimilation_time,
                         screen=True,
                         model_lon=model_lon,
                         model_lat=model_lat)

m = len(obs_data)  # m is the number of valid observations
y = np.array(obs_data.iloc[:, 2]).reshape([m, 1])  # observational vector
H = np.zeros([m, Ns])  # H operator
O = np.zeros([m, m])  # observational error covariance matrix
sigma = np.zeros([m])  # observational error vector
obs_loc_mapped = np.zeros([2, m])  # the location of observation after mapping

for i_obs in range(m):

    map_lon = asl.find_nearest(obs_data.iloc[i_obs, 0], model_lon)
    map_lat = asl.find_nearest(obs_data.iloc[i_obs, 1], model_lat)

    obs_loc_mapped[:, i_obs] = model_lon[map_lon], model_lat[map_lat]

    ### assign the H operator ###
    H_map_index = int(map_lat * Nlon + map_lon)
    H[i_obs, H_map_index] = 1

    ### assign the Observation error covariance matrix ###
    sigma[i_obs] = leol.assign_obs_error(y[i_obs])
    O[i_obs, i_obs] = sigma[i_obs]**2

system_log.info('%s observations received' % (m))

#########################################################
###               calculate Kalman Gain               ###
X_f_extend = np.sum(X_f_3d[:, :, 0, :, :], axis=1).reshape(
    [Ne_extend,
     Ns])  # convert [Ne_extend, Nspec, Nlev, Nlat, Nlon] to [Ne_extend, Ns]

x_f_mean = np.nanmean(X_f_extend, axis=0)
X_pertubate = (X_f_extend - x_f_mean).T
system_log.debug(f'dim of X_pertubate : {X_pertubate.shape}')
U = H @ X_pertubate
system_log.debug(f'dim of U : {U.shape}')

if Config['Assimilation'][assimilation_scheme]['use_localization']:

    start_local = datetime.now()
    system_log.info(
        'Localization enabled, distance threshold : %s km' %
        (Config['Assimilation'][assimilation_scheme]['distance_threshold']))

    local = leal.Local_class(
        model_lon,
        model_lat,
        obs_loc_mapped[0, :],
        obs_loc_mapped[1, :],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'],
        meshgrid1=True)
    L1 = local.calculate()  # dim : Ns * m
    system_log.debug((f'Dims of Localization 1 : {L1.shape}'))

    local = leal.Local_class(
        obs_loc_mapped[0, :], obs_loc_mapped[1, :], obs_loc_mapped[0, :],
        obs_loc_mapped[1, :],
        Config['Assimilation'][assimilation_scheme]['distance_threshold'])
    L2 = local.calculate()  # dim : m * m
    system_log.debug((f'Dims of Localization 2 : {L2.shape}'))

    system_log.debug('localization took %.2f s' %
                     ((datetime.now() - start_local).total_seconds()))

    K = (np.multiply(X_pertubate @ U.T), L1) / (Ne_extend - 1) @ \
        inv(np.multiply(U @ U.T, L2) / (Ne_extend - 1) + O)

else:

    K = (X_pertubate @ U.T) / (Ne_extend - 1) @ \
        inv(U @ U.T / (Ne_extend - 1) + O)

system_log.info('Kalman Gain calculated')

######################################################
###   calculate poerteriors and write them back    ###
###   to model restart file                        ###
if Config['Model'][model_scheme]['run_type'] == 'ensemble_extend':

    ### convert to 3D and write back to restart file ###
    if Config['Assimilation'][assimilation_scheme]['write_restart']:

        pool = mp.Pool(Config['Assimilation']['node']['core_demand'])
        results = []

        for i_time in range(len(time_set)):
            for i_ensem in range(ensemble_set[i_time]):

                model_restart_path = os.path.join(
                    Config['Model'][model_scheme]['path']['model_output_path'],
                    Config['Model'][model_scheme]['run_project'],
                    run_id_write[i_time][i_ensem], 'restart')

                if not os.path.exists(model_restart_path):
                    os.makedirs(model_restart_path)

                n_count = sum(ensemble_set[:i_time]) + i_ensem

                X_a = np.zeros([Ne_extend, Ns])
                X_a_3d = np.zeros([Ne_extend, Nspec, Nlev, Nlat, Nlon])
                for i_ensem in range(Ne_extend):

                    y_i = np.zeros([m, 1])
                    for j in range(m):
                        y_i[j, 0] = y[j] + np.random.normal(
                            loc=0, scale=sigma[j], size=1)

                    x_f_i = X_f_extend[i_ensem].reshape([Ns, 1])

                    X_a[i_ensem, :] = asl.calculate_posteriori(x_f=x_f_i,
                                                               K=K,
                                                               y=y_i,
                                                               H=H)

                X_a_3d[n_count, :] = leal.convert2full(
                    posteriori_2d=X_a[n_count],
                    Nspec=Nspec,
                    nlon=Nlon,
                    nlat=Nlat,
                    nlevel=Nlev,
                    mass_partition=mass_partition[i_time],
                    spec_partition=spec_partition[i_time])

                # leal.write_new_nc(posteriori=X_a_3d[n_count, :],
                #                  restart_path=model_output_path,
                #                  run_id=run_id_write[i_time][i_ensem],
                #                  time=assimilation_time)

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

                info = [
                    X_a_3d[n_count, :],
                    os.path.join(
                        Config['Model'][model_scheme]['path']
                        ['model_output_path'],
                        Config['Model'][model_scheme]['run_project']),
                    run_id_write[i_time][i_ensem], assimilation_time
                ]

                results.append(
                    pool.apply_async(func=leal.write_new_nc_patch,
                                     args=(info, )))

        results = [p.get() for p in results]

########################################################
###      save the variables for post processing      ###
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
                                      mass_partition=np.nanmean(mass_partition,
                                                                axis=0),
                                      spec_partition=np.nanmean(spec_partition,
                                                                axis=0)),
                    axis=0)

    if Config['Model'][model_scheme]['run_type'] == 'ensemble_extend':

        if Config['Assimilation']['post_process']['save_method'] == 'npy':

            asl.save2npy(dir_name=save_variables_dir,
                         variables={
                             'posteriori': np.sum(np.mean(X_a_3d, axis=0),
                                                  axis=0),
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
###   tell main branch i am finished   ###
system_log.info('Assimilation finished, took {:.2f} s'.format(
    (datetime.now() - project_start_run_time).total_seconds()))
stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 100}})
