'''
Autor: Mijie Pang
Date: 2023-04-22 19:27:07
LastEditTime: 2023-10-23 19:48:11
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
from system_lib import read_json
from tool.china_map import my_map as mmp
from tool.metrics import MetricTwoD

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Model = read_json(path=config_dir + '/Model.json')
Assimilation = read_json(path=config_dir + '/Assimilation.json')
Info = read_json(path=config_dir + '/Info.json')
Status = read_json(path=status_path)

model_scheme = Model['scheme']['name']
assimilation_scheme = Assimilation['scheme']['name']

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

run_project = 'ChinaDust20210328_v22'
start_time = datetime.strptime('2021-03-27 01:00:00', '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime('2021-03-29 23:00:00', '%Y-%m-%d %H:%M:%S')
model_output_dir = os.path.join(Info['path']['output_path'], run_project,
                                'model_run')
output_dir = os.path.join(Info['path']['output_path'], run_project,
                          'model_run', 'snapshot', 'conc-sfc')

# run_project = 'ChinaDust20210328'
# start_time = datetime.strptime('2021-03-28 12:00:00', '%Y-%m-%d %H:%M:%S')
# end_time = datetime.strptime('2021-03-28 12:00:00', '%Y-%m-%d %H:%M:%S')
# model_output_dir = Model[model_scheme]['path']['backup_path'] + '/' + run_project
# output_dir = '/home/pangmj/Data/variable_cache/tmp'

date_range = pd.date_range(start_time, end_time, freq='1H')
spec = ['dust_ff', 'dust_f', 'dust_c', 'dust_cc', 'dust_ccc']
bounds = [0, 100, 300, 600, 1000, 2000, 3000, 4000, 999999]
colors = [
    '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
    '#0000b3', '#000000'
]
extent = [85, 135, 30, 50]

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def main(num, data_type='restart', plot_pic=True):

    ### read model simulation results ###
    output = np.zeros([Ne, len(simu_lat), len(simu_lon)])
    model_flag = False
    if data_type == 'restart':

        for i_ensem in range(Ne):

            run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
            path = os.path.join(
                model_output_dir, run_id, 'restart', 'LE_%s_state_%s.nc' %
                (run_id, date_range[num].strftime('%Y%m%d_%H%M')))

            if os.path.exists(path):
                model_flag = True

                with nc.Dataset(path) as nc_obj:

                    for i_spec in range(len(spec)):
                        output[i_ensem, :, :] += nc_obj.variables['c'][i_spec,
                                                                       0, :, :]

    elif data_type == 'output':

        for i_ensem in range(Ne):

            run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
            path = os.path.join(
                model_output_dir, run_id, 'output', 'LE_%s_conc-3d_%s.nc' %
                (run_id, date_range[num].strftime('%Y%m%d')))

            if os.path.exists(path):
                model_flag = True

                with nc.Dataset(path) as nc_obj:

                    time = nc_obj.variables['time']
                    time = nc.num2date(time[:], time.units)
                    time = [str(time[i_time]) for i_time in range(len(time))]
                    idx = time.index(
                        date_range[num].strftime('%Y-%m-%d %H:%M:%S'))

                    for i_spec in range(len(spec)):
                        output[i_ensem, :, :] += nc_obj.variables[
                            spec[i_spec]][idx, 0, :, :] * (10**9)

    if model_flag:

        ##############################
        ###    read observation    ###
        output_mean = np.mean(output, axis=0)
        obs_flag = False
        obs_path = os.path.join(
            Assimilation['path']['observation_path'],
            Assimilation['observation']['data_type'],
            'BC_PM10_%sUTC.csv' % (date_range[num].strftime('%Y_%m_%d_%H%M')))
        if os.path.exists(obs_path):

            obs_flag = True

            obs_data = pd.read_csv(obs_path, header=None)
            obs_lon = obs_data.iloc[:, 0]
            obs_lat = obs_data.iloc[:, 1]
            obs_val = obs_data.iloc[:, 2]

            mapped_obs = []
            mapped_simu = []
            count = 0
            for lon_o, lat_o in zip(obs_lon, obs_lat):
                map_lon = pol.find_nearest(lon_o, simu_lon)
                map_lat = pol.find_nearest(lat_o, simu_lat)
                if not np.isnan(map_lon) and not np.isnan(map_lat):
                    mapped_obs.append(obs_val[count])
                    mapped_simu.append(output_mean[map_lat, map_lon])
                count += 1

            metric = MetricTwoD(mapped_obs, mapped_simu)
            rmse = metric.calculate('rmse')
            nmb = metric.calculate('nmb')

        ##################################
        ###          for plot          ###
        if plot_pic:

            plot = mmp(quick=False,
                       map2=False,
                       title='model run : ' +
                       date_range[num].strftime('%Y-%m-%d %H:%M'),
                       extent=extent,
                       bounds=bounds[:-1],
                       colors=colors,
                       colorbar_shrink=0.6)

            output_mean[output_mean < 1e-6] = np.nan
            plot.contourf(simu_lon, simu_lat, output_mean, levels=bounds)

            if obs_flag:
                # plot.scatter(obs_lon, obs_lat, obs_val, meshgrid=False, size=12)
                plot.text([0.37, 0.9], text='RMSE : ' + str(round(rmse, 1)))
                plot.text([0.37, 0.82],
                          text=' NMB : ' + str(round(nmb * 100, 1)) + '%')

            plot.save(date_range[num].strftime('%Y%m%d_%H%M'), path=output_dir)
            plot.close()

        if obs_flag:
            return [date_range[num].strftime('%Y%m%d_%H%M'), rmse, nmb]
        else:
            return [date_range[num].strftime('%Y%m%d_%H%M'), np.nan, np.nan]


start_t = datetime.now()

num_cores = 12
pool = mp.Pool(num_cores)
numbers = np.arange(0, len(date_range))
results = [pool.apply_async(main, args=(num, )) for num in numbers]
results = [p.get() for p in results]

# metric_data = pd.DataFrame(results)
# metric_data.columns = ['Time', 'RMSEs', 'NMBs']

# metric_data[['Time', 'RMSEs']].to_csv(output_dir + '/metrics_rmse.csv',
#                                       index=None)
# metric_data[['Time', 'NMBs']].to_csv(output_dir + '/metrics_nmb.csv',
#                                      index=None)

end_t = datetime.now()
elapsed_sec = (end_t - start_t).total_seconds()
print('process time : %.2f s' % (elapsed_sec))
