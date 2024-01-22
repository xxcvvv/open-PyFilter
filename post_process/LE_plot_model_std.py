'''
Autor: Mijie Pang
Date: 2023-04-22 19:27:24
LastEditTime: 2023-10-23 20:06:41
Description: 
'''
import os
import sys
import numpy as np
import pandas as pd
import netCDF4 as nc
from datetime import datetime
import multiprocessing as mp

sys.path.append('../')
from system_lib import read_json
from tool.china_map import my_map as mmp

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Model = read_json(path=config_dir + '/Model.json')
Assimilation = read_json(path=config_dir + '/Assimilation.json')
Status = read_json(path=status_path)

model_scheme = Model['scheme']['name']
assi_scheme = Assimilation['scheme']['name']

run_project = 'ChinaDust20210328'
model_output_path = os.path.join(Model[model_scheme]['path']['backup_path'],
                                 run_project)
output_dir = os.path.join(Assimilation['path']['results_path'], run_project,
                          'model_var')

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

Ne = Model[model_scheme]['ensemble_number']
nlon = Model[model_scheme]['nlon']
nlat = Model[model_scheme]['nlat']
Nx = nlon * nlat
simu_lon = np.arange(Model[model_scheme]['start_lon'],
                     Model[model_scheme]['end_lon'],
                     Model[model_scheme]['res_lon'])
simu_lat = np.arange(Model[model_scheme]['start_lat'],
                     Model[model_scheme]['end_lat'],
                     Model[model_scheme]['res_lat'])
iteration_num = Model[model_scheme]['iteration_num']
start_time = datetime.strptime('2021-03-29 01:00:00', '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime('2021-03-29 23:00:00', '%Y-%m-%d %H:%M:%S')
date_range = pd.date_range(start_time, end_time, freq='1H')

spec = ['dust_ff', 'dust_f', 'dust_c', 'dust_cc', 'dust_ccc']


def main(time, output_type='restart'):

    print(time)

    #############################################
    ###           read model output           ###
    output = np.zeros([Ne, len(simu_lat), len(simu_lon)])

    if output_type == 'output':

        for i_ensemble in range(Ne):

            run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensemble)
            path = os.path.join(
                model_output_path, run_id, 'output',
                'LE_%s_conc-sfc_%s.nc' % (run_id, time.strftime('%Y%m%d')))

            with nc.Dataset(path) as nc_obj:

                output_time = nc_obj.variables['time']
                output_time = nc.num2date(output_time[:], output_time.units)
                output_time = [
                    str(output_time[i_time])
                    for i_time in range(len(output_time))
                ]
                idx = output_time.index(time.strftime('%Y-%m-%d %H:%M:%S'))

                for j in range(len(spec)):
                    output[i_ensemble, :, :] += nc_obj.variables[spec[j]][
                        idx, 0, :, :] * (10**9)

    elif output_type == 'restart':

        for i_ensemble in range(Ne):

            run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensemble)
            path = os.path.join(
                model_output_path, run_id, 'restart',
                'LE_%s_state_%s.nc' % (run_id, time.strftime('%Y%m%d_%H%M')))

            with nc.Dataset(path) as nc_obj:

                for j in range(len(spec)):
                    output[i_ensemble, :, :] += nc_obj.variables['c'][j,
                                                                      0, :, :]

    output_var = np.array(
        [[np.var(output[:, a, b]) for b in range(len(simu_lon))]
         for a in range(len(simu_lat))])
    output_std = output_var**(0.5)

    output_var[output_var < 1e-9] = np.nan
    output_std[output_std < 1e-9] = np.nan

    ########################################
    ###         read observation         ###
    if not os.path.exists(Assimilation['path']['observation_path'] + '/' +
                          Assimilation['observation']['data_type'] +
                          '/BC_PM10_' + time.strftime('%Y_%m_%d_%H%M') +
                          'UTC.csv'):
        obs_flag = False
    else:
        obs_flag = True

    if obs_flag:

        obs_data = pd.read_csv(Assimilation['path']['observation_path'] + '/' +
                               Assimilation['observation']['data_type'] +
                               '/BC_PM10_' + time.strftime('%Y_%m_%d_%H%M') +
                               'UTC.csv',
                               header=None)
        obs_lon = obs_data.iloc[:, 0]
        obs_lat = obs_data.iloc[:, 1]
        obs_val = obs_data.iloc[:, 2]

    ################################################
    ###                 for plot                 ###
    # bounds = [1e-99, 1, 10, 100, 1000, 5000, 10000, 50000, 99999999999]
    # bounds=[bounds[i_bd]**(0.5) for i_bd in range(len(bounds))]
    bounds = [1e-99, 1, 5, 10, 20, 50, 100, 200, 9999999999]
    colors = [
        '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
        '#0000b3', '#000000'
    ]
    colors = [
        '#d1d1e0', '#b3b3cc', '#7575a3', '#5c5c8a', '#47476b', '#33334c',
        '#29293d', '#000000'
    ]

    plot = mmp(quick=False,
               map2=False,
               title='Ensemble std. : ' + time.strftime('%Y-%m-%d %H:%M'),
               bounds=bounds,
               extent=[85, 135, 30, 50],
               colors=colors,
               colorbar_shrink=0.6)

    plot.contourf(simu_lon, simu_lat, output_std, levels=bounds)

    if obs_flag:

        plot.scatter(obs_lon,
                     obs_lat,
                     obs_val,
                     meshgrid=False,
                     size=8,
                     bounds=[0, 100, 300, 600, 1000, 2000, 3000, 4000, 9999999],
                     colors=[
                         '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA',
                         '#1414FA', '#0000b3', '#000000'
                     ])

    plot.save(time.strftime('%Y%m%d_%H%M'), path=output_dir)
    plot.close()


start_t = datetime.now()

num_cores = int(mp.cpu_count())
pool = mp.Pool(num_cores)
results = [pool.apply_async(main, args=(time, )) for time in date_range]
results = [p.get() for p in results]

end_t = datetime.now()
elapsed_sec = (end_t - start_t).total_seconds()
print('process time : %.2f s' % (elapsed_sec))
