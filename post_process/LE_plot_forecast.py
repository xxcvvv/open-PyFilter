'''
Autor: Mijie Pang
Date: 2023-04-22 19:25:41
LastEditTime: 2023-12-11 11:46:21
Description: 
'''
import os
import sys
import numpy as np
import pandas as pd
import netCDF4 as nc
import multiprocessing as mp
from datetime import datetime

import post_lib as pol

sys.path.append('../')
from system_lib import read_json, read_json_dict
from tool.china_map import my_map as mmp
from tool.metrics import MetricTwoD

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Config = read_json_dict(config_dir, 'Model.json', 'Assimilation.json',
                        'Observation.json')
Status = read_json(path=status_path)

spec = ['dust_ff', 'dust_f', 'dust_c', 'dust_cc', 'dust_ccc']
bounds = [0, 100, 300, 600, 1000, 2000, 3000, 4000, 999999]
colors = [
    '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
    '#0000b3', '#000000'
]
extent = [85, 135, 30, 50]

model_scheme = Config['Model']['scheme']['name']
assi_scheme = Config['Assimilation']['scheme']['name']

Ne = Config['Model'][model_scheme]['ensemble_number']
model_lon = np.arange(Config['Model'][model_scheme]['start_lon'],
                      Config['Model'][model_scheme]['end_lon'],
                      Config['Model'][model_scheme]['res_lon'])
model_lat = np.arange(Config['Model'][model_scheme]['start_lat'],
                      Config['Model'][model_scheme]['end_lat'],
                      Config['Model'][model_scheme]['res_lat'])
iteration_num = Config['Model'][model_scheme]['iteration_num']

start_time = datetime.strptime(Status['model']['start_time'],
                               '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime(Status['model']['end_time'], '%Y-%m-%d %H:%M:%S')
time_range = pd.date_range(
    start_time,
    end_time,
    freq=Config['Model'][model_scheme]['output_time_interval'])

results_dir = os.path.join(Config['Info']['path']['output_path'],
                           Config['Model'][model_scheme]['run_project'],
                           Config['Assimilation'][assi_scheme]['project_name'])

########################################################
###                 make directories                 ###
forecast_plot_dir = results_dir + '/forecast/'+ \
                    start_time.strftime('%Y%m%d_%H%M')

if not os.path.exists(forecast_plot_dir):
    os.makedirs(forecast_plot_dir)


def main(forecast_time):

    ##########################################
    ###          get model output          ###
    if Config['Model'][model_scheme]['run_type'] == 'ensemble':

        output = np.zeros([Ne, len(model_lat), len(model_lon)])

        for i in range(Ne):

            run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i)
            path = os.path.join(
                forecast_plot_dir, 'forecast_files', run_id, 'output',
                'LE_%s_conc-sfc_%s.nc' %
                (run_id, forecast_time.strftime('%Y%m%d')))

            with nc.Dataset(path) as nc_obj:

                time = nc_obj.variables['time']
                time = nc.num2date(time[:], time.units)
                time = [str(time[i_time]) for i_time in range(len(time))]
                idx = time.index(forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

                for j in range(len(spec)):
                    output[i, :, :] += nc_obj.variables[spec[j]][
                        idx, 0, :, :] * (10**9)

        output_mean = np.mean(output, axis=0)

    elif Config['Model'][model_scheme]['run_type'] == 'single':

        output_mean = np.zeros([len(model_lat), len(model_lon)])

        run_id = Config['Assimilation'][assi_scheme]['project_name']
        path = os.path.join(
            forecast_plot_dir, 'forecast_files', 'output',
            'LE_%s_conc-3d_%s.nc' % (run_id, forecast_time.strftime('%Y%m%d')))

        with nc.Dataset(path) as nc_obj:

            time = nc_obj.variables['time']
            time = nc.num2date(time[:], time.units)
            time = [str(time[i_time]) for i_time in range(len(time))]
            idx = time.index(forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

            for j in range(len(spec)):
                output_mean[:, :] += nc_obj.variables[spec[j]][idx,
                                                               0, :, :] * (10**
                                                                           9)

    elif Config['Model'][model_scheme]['run_type'] == 'ensemble_extend':

        time_set = Config['Assimilation'][assi_scheme]['time_set']
        ensemble_set = Config['Assimilation'][assi_scheme]['ensemble_set']
        ensemble_set = [
            int(ensemble_set[i_ensem]) for i_ensem in range(len(ensemble_set))
        ]
        Ne_extend = sum(ensemble_set)
        run_id = [[
            't_' + str(time_set[i_time]) + '_e_%02d' % (i_ensem)
            for i_ensem in range(ensemble_set[i_time])
        ] for i_time in range(len(time_set))]

        output = np.zeros([Ne_extend, len(model_lat), len(model_lon)])

        for i_time in range(len(time_set)):
            for i_ensem in range(ensemble_set[i_time]):

                Ne_count = sum(ensemble_set[:i_time]) + i_ensem

                path = os.path.join(
                    forecast_plot_dir, 'forecast_files',
                    run_id[i_time][i_ensem], 'output',
                    'LE_%s_conc-3d_%s.nc' % (run_id[i_time][i_ensem],
                                             forecast_time.strftime('%Y%m%d')))

                with nc.Dataset(path) as nc_obj:

                    time = nc_obj.variables['time']
                    time = nc.num2date(time[:], time.units)
                    time = [str(time[i_time]) for i_time in range(len(time))]
                    idx = time.index(
                        forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

                    for j in range(len(spec)):
                        output[Ne_count, :, :] += nc_obj.variables[spec[j]][
                            idx, 0, :, :] * (10**9)

        output_mean = np.mean(output, axis=0)

    ##########################################################
    ###   get observation data and calculate the metrics   ###
    obs_flag = False
    file_path = os.path.join(
        Config['Observation']['path'], Config['Observation']['bc_pm10']['dir'],
        'BC_PM10_%sUTC.csv' % (forecast_time.strftime('%Y_%m_%d_%H%M')))

    if os.path.exists(file_path):

        obs_flag = True

        obs_data = pd.read_csv(file_path, header=None)
        obs_lon = obs_data.iloc[:, 0]
        obs_lat = obs_data.iloc[:, 1]
        obs_val = obs_data.iloc[:, 2]

        mapped_obs = []
        mapped_simu = []
        count = 0
        for lon_o, lat_o in zip(obs_lon, obs_lat):
            map_lon = pol.find_nearest(lon_o, model_lon)
            map_lat = pol.find_nearest(lat_o, model_lat)
            if not np.isnan(map_lon) and not np.isnan(map_lat):
                mapped_obs.append(obs_val[count])
                mapped_simu.append(output_mean[map_lat, map_lon])
            count += 1

        metric = MetricTwoD(mapped_obs, mapped_simu)
        rmse = metric.calculate('rmse')
        nmb = metric.calculate('nmb')

    ##############################################
    ###                for plot                ###
    plot = mmp(quick=False,
               map2=False,
               title=forecast_time.strftime('%Y%m%d_%H%M'),
               bounds=bounds[:-1],
               extent=extent,
               colors=colors,
               colorbar_shrink=0.6)

    output_mean[output_mean <= 1e-9] = np.nan
    plot.contourf(model_lon, model_lat, output_mean, levels=bounds)

    if obs_flag:
        plot.text([0.37, 0.9], text='RMSE : %.1f' % (rmse))
        plot.text([0.37, 0.82], text=' NMB : %.1f ' % (nmb * 100) + '%')

        if Config['Model'][model_scheme]['post_process']['with_observation']:
            plot.scatter(obs_lon, obs_lat, obs_val, meshgrid=False, size=48)

    plot.save(forecast_time.strftime('%Y%m%d_%H%M'), path=forecast_plot_dir)
    plot.close()

    if obs_flag:
        return [forecast_time.strftime('%Y%m%d_%H%M'), rmse]
    else:
        return [forecast_time.strftime('%Y%m%d_%H%M'), np.nan]


#################################
###    parallel processing    ###
start_t = datetime.now()

num_cores = Config['Model'][model_scheme]['post_process']['run_spec'][1]
pool = mp.Pool(num_cores)
results = [
    pool.apply_async(main, args=(forecast_time, ))
    for forecast_time in time_range
]
results = [p.get() for p in results]

end_t = datetime.now()
elapsed_sec = (end_t - start_t).total_seconds()
print('process time : {:.2f} s'.format(elapsed_sec))
