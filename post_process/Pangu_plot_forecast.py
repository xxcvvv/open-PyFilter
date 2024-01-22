'''
Autor: Mijie Pang
Date: 2023-08-20 21:10:57
LastEditTime: 2023-08-27 09:36:15
Description: 
'''
import os
import sys
import numpy as np
import pandas as pd
import netCDF4 as nc
import multiprocessing as mp
from datetime import datetime

sys.path.append('../')
import system_lib as stl
from tool.global_map import my_globe

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Config = stl.read_json_dict(config_dir, 'Model.json', 'Assimilation.json',
                            'Observation.json', 'Info.json')
Status = stl.read_json(path=status_path)

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### initialize parameters ###
model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']

model_lon = np.linspace(0.125, 359.875, 1440)
model_lat = np.linspace(90, -90, 721)

start_time = datetime.strptime(Status['model']['start_time'],
                               '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime(Status['model']['end_time'], '%Y-%m-%d %H:%M:%S')
forecast_time_range = pd.date_range(
    start_time,
    end_time,
    freq=Config['Model'][model_scheme]['output_time_interval'])[1:]

data_dir = os.path.join(
    Config['Info']['path']['output_path'],
    Config['Model'][model_scheme]['run_project'],
    Config['Assimilation'][assimilation_scheme]['project_name'], 'forecast',
    start_time.strftime('%Y%m%d_%H%M'))

output_dir = os.path.join(data_dir, 'snapshot')

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


### plot global 2m temperature ###
def plot_t2m(forecast_time, model_lon=model_lon, model_lat=model_lat):

    with nc.Dataset(os.path.join(data_dir, 'output_surface.nc')) as nc_obj:

        time = list(nc_obj.variables['time'][:])
        idx = time.index(forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

        t2m = nc_obj.variables['t2m'][idx, :, :]

    plot = my_globe(title='t2m : %s CST' %
                    (forecast_time.strftime('%Y-%m-%d %H:%M')))
    plot.add_colorbar(bounds=[-30, -20, -10, 0, 10, 20, 30],
                      cmap='jet',
                      extend='both',
                      norm='continous')
    plot.contourf(model_lon, model_lat, t2m - 273, levels=100)
    plot.save('t2m_%s.png' % (forecast_time.strftime('%Y%m%d_%H%M')),
              path=output_dir)
    plot.close()


### plot global wind speed field ###
def plot_ws(forecast_time, model_lon=model_lon, model_lat=model_lat):

    with nc.Dataset(os.path.join(data_dir, 'output_surface.nc')) as nc_obj:

        time = list(nc_obj.variables['time'][:])
        idx = time.index(forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

        u10 = nc_obj.variables['u10'][idx, :, :]
        v10 = nc_obj.variables['v10'][idx, :, :]

    wind_speed = np.sqrt(u10**2 + v10**2)

    plot = my_globe(title='wind speed : %s CST' %
                    (forecast_time.strftime('%Y-%m-%d %H:%M')))
    plot.add_colorbar(bounds=[0, 2, 4, 6, 8, 10, 12, 16, 20],
                      cmap='jet',
                      extend='max',
                      norm='continous')
    plot.contourf(model_lon,
                  model_lat,
                  wind_speed,
                  levels=100,
                  hide_line=True,
                  alpha=0.8)
    plot.save('ws_%s.png' % (forecast_time.strftime('%Y%m%d_%H%M')),
              path=output_dir)
    plot.close()


### plot global wind direction field ###
def plot_wd(forecast_time, model_lon=model_lon, model_lat=model_lat):

    with nc.Dataset(os.path.join(data_dir, 'output_surface.nc')) as nc_obj:

        time = list(nc_obj.variables['time'][:])
        idx = time.index(forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

        u10 = nc_obj.variables['u10'][idx, :, :]
        v10 = nc_obj.variables['v10'][idx, :, :]

    plot = my_globe(title='wind field : %s CST' %
                    (forecast_time.strftime('%Y-%m-%d %H:%M')))
    plot.quiver(
        model_lon,
        model_lat,
        u10,
        v10,
        lon_fraction=30,
        lat_fraction=30,
        scale=600,
        width=0.001,
        headwidth=6,
    )
    plot.quiverkey(length=20, label='20 m/s', font={'size': 10})
    plot.save('wd_%s.png' % (forecast_time.strftime('%Y%m%d_%H%M')),
              path=output_dir)
    plot.close()


### plot global mean sea pressure level ###
def plot_msl(forecast_time, model_lon=model_lon, model_lat=model_lat):

    with nc.Dataset(os.path.join(data_dir, 'output_surface.nc')) as nc_obj:

        time = list(nc_obj.variables['time'][:])
        idx = time.index(forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

        msl = nc_obj.variables['msl'][idx, :, :]

    plot = my_globe(title='msl : %s CST' %
                    (forecast_time.strftime('%Y-%m-%d %H:%M')))
    plot.add_colorbar(bounds=[940, 960, 980, 1000, 1020, 1040, 1060],
                      cmap='jet',
                      extend='both',
                      norm='continous',
                      label='hpa')
    plot.contourf(model_lon,
                  model_lat,
                  msl / 100,
                  levels=100,
                  hide_line=True,
                  alpha=0.8)
    plot.save('msl_%s.png' % (forecast_time.strftime('%Y%m%d_%H%M')),
              path=output_dir)
    plot.close()


###    parallel processing    ###
start_t = datetime.now()

num_cores = Config['Model'][model_scheme]['post_process']['run_spec'][1]
pool = mp.Pool(num_cores)

for i_time in range(len(forecast_time_range)):

    pool.apply_async(plot_t2m, args=(forecast_time_range[i_time], ))
    pool.apply_async(plot_ws, args=(forecast_time_range[i_time], ))
    pool.apply_async(plot_wd, args=(forecast_time_range[i_time], ))
    pool.apply_async(plot_msl, args=(forecast_time_range[i_time], ))

pool.close()
pool.join()

elapsed_sec = (datetime.now() - start_t).total_seconds()
print('process time : {:.2f} s'.format(elapsed_sec))
