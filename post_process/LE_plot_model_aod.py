'''
Autor: Mijie Pang
Date: 2023-02-05 16:30:16
LastEditTime: 2023-07-28 09:11:31
Description: 
'''
import sys
import numpy as np
import pandas as pd
import netCDF4 as nc
from datetime import datetime, timedelta

sys.path.append('../')
from system_lib import read_json
from tool.china_map import my_map as mmp

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Model = read_json(path=config_dir + '/Model.json')
Assimilation = read_json(path=config_dir + '/Assimilation.json')
Info = read_json(path=config_dir + '/Info.json')
Status = read_json(path=status_path)

model_scheme = Model['scheme']['name']
assimilation_scheme = Assimilation['scheme']['name']

run_project = Model[model_scheme]['run_project']
project_name = Assimilation[assimilation_scheme]['project_name']
Ne = Model[model_scheme]['ensemble_number']

# run_project = 'ChinaDust20210328'
# project_name = 'L500'
# Ne = 32

# base_time = datetime.strptime('2021-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
predict_start_time = datetime.strptime('2021-03-28 00:00:00',
                                       '%Y-%m-%d %H:%M:%S')
target_time = datetime.strptime('2021-03-28 03:00:00', '%Y-%m-%d %H:%M:%S')
target_time_cst = target_time + timedelta(hours=8)

predict_dir=Info['path']['output_path'] + '/' + \
                run_project+'/'+project_name+ '/forecast/' + \
                predict_start_time.strftime('%Y%m%d_%H%M')
model_output_path = predict_dir + '/forecast_files'

model_data_path = model_output_path+'/iter_00_ensem_00/' + \
            'output/LE_iter_00_ensem_00_aod2_'+target_time.strftime('%Y%m%d')+'.nc'
nc_obj = nc.Dataset(model_data_path)
model_lon = nc_obj.variables['longitude'][:]
model_lat = nc_obj.variables['latitude'][:]

obs_data_path='/home/pangmj/Data/pyFilter/MODIS/BC_DB_coarse_'+\
                target_time.strftime('%Y_%m_%d_%H%M')+'UTC.csv'

bounds = [0, 0.1, 0.3, 0.6, 1, 2, 3, 4, 9999]
colors = [
    '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA', '#0000b3',
    '#000000'
]
extent = [85, 135, 30, 50]


def get_obs_aod(path=''):

    data = pd.read_csv(path)
    lon = data.loc[:, 'lons']
    lat = data.loc[:, 'lats']
    aod = data.loc[:, 'AOD550nm']
    print('got observation data')
    return lon, lat, aod


def get_model_aod():

    aod = np.zeros([Ne, len(model_lat), len(model_lon)])
    for i in range(Ne):

        run_id = 'iter_00_ensem_%02d' % i
        data_path = model_output_path + '/' + run_id + '/output/LE_' + run_id + \
                    '_aod2_'+target_time.strftime('%Y%m%d')+'.nc'
        nc_obj = nc.Dataset(data_path)
        aod[i, :, :] = nc_obj.variables['aod_550nm'][2, :, :]

    aod = np.nanmean(aod, axis=0)
    print('got model data')
    return aod


def find_nearest(value='', array=[]):

    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return idx


def calculate_rmse(model=[], obs=[]):

    model_lon, model_lat, model_aod = model
    obs_lon, obs_lat, obs_aod = obs
    model_aod[model_aod < 0] = 0
    map_aod = np.zeros([len(model_lat), len(model_lon)])
    for i in range(len(obs_aod)):
        map_aod[find_nearest(value=obs_lat[i], array=model_lat),
                find_nearest(value=obs_lon[i], array=model_lon)] = obs_aod[i]

    mask_map = map_aod > 1e-6
    mask_model = model_aod > 1e-6
    mask = np.zeros([len(model_lat), len(model_lon)])
    for i in range(len(model_lon)):
        for j in range(len(model_lat)):
            mask[j, i] = mask_map[j, i] or mask_model[j, i]
    mask = mask.astype(bool)

    rmse = np.sqrt(np.nanmean((map_aod[mask] - model_aod[mask])**2))
    return rmse


### plot ###
def plot(model=[], rmse=''):

    model_lon, model_lat, model_aod = model
    model_aod[model_aod < 1e-6] = np.nan

    plot = mmp(quick=False,
               map2=False,
               title='Predicted AOD of ' + project_name + ' : ' +
               target_time_cst.strftime('%Y-%m-%d %H:%M') + ' CST',
               bounds=bounds,
               colorbar_shrink=0.6,
               extent=extent,
               colors=colors)
    plot.contourf(model_lon, model_lat, model_aod, levels=bounds)
    plot.text([0.37, 0.9], text='RMSE : ' + str(round(rmse, 3)))
    plot.save('Model_predict_AOD_' + project_name + '_' +
              target_time.strftime('%Y%m%d_%H%M'),
              path=predict_dir)
    plot.close()


obs_lon, obs_lat, obs_aod = get_obs_aod(path=obs_data_path)
model_aod = get_model_aod()
rmse = calculate_rmse(model=[model_lon, model_lat, model_aod],
                      obs=[obs_lon, obs_lat, obs_aod])
print('rmse = ' + str(rmse))
plot(model=[model_lon, model_lat, model_aod], rmse=rmse)
