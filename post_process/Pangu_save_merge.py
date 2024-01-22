'''
Autor: Mijie Pang
Date: 2023-08-20 21:24:35
LastEditTime: 2023-08-24 22:47:30
Description: 
'''
import os
import sys
import numpy as np
import pandas as pd

sys.path.append('../')
import system_lib as stl
from tool.pack import NcProduct

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Config = stl.read_json_dict(config_dir, 'Model.json', 'Assimilation.json',
                            'Info.json', 'Input.json')
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']
assi_scheme = Config['Assimilation']['scheme']['name']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### initialize parameters ###
model_lon = np.linspace(0.125, 359.875, 1440)
model_lat = np.linspace(90, -90, 721)

ensemble_number = Config['Model'][model_scheme]['ensemble_number']
time_range = pd.date_range(
    Status['model']['start_time'],
    Status['model']['end_time'],
    freq=Config['Model'][model_scheme]['output_time_interval'])

data_dir = os.path.join(Config['Info']['path']['output_path'],
                        Config['Model'][model_scheme]['run_project'],
                        Config['Assimilation'][assi_scheme]['project_name'],
                        'forecast', time_range[0].strftime('%Y%m%d_%H%M'),
                        'output')

### define the run ids ###
if Config['Model'][model_scheme]['run_type'] == 'ensemble':
    run_ids = ['pangu_%04d' % (i_ensem) for i_ensem in range(ensemble_number)]

elif Config['Model'][model_scheme]['run_type'] == 'single':
    run_ids = ['base']

print('list of run id : ', run_ids)

###################################################
###           start to merge the data           ###

####################################################################
# product 1 : the surface data
if 'surface' in Config['Model'][model_scheme]['post_process']['product']:

    print('### start to produce surface product ###')

    ### initialize the netcdf product ###
    nc_product = NcProduct(os.path.join(data_dir, 'surface.nc'),
                           Model=Config['Model'],
                           Assimilation=Config['Assimilation'],
                           Info=Config['Info'])

    nc_product.define_dimension(
        longitude=Config['Model'][model_scheme]['nlon'],
        latitude=Config['Model'][model_scheme]['nlat'],
        time=None)

    nc_product.define_variable(longitude=['f4', 'longitude'],
                               latitude=['f4', 'latitude'],
                               time=['S19', 'time'],
                               msl=['latitude', 'longitude'],
                               u10=['time', 'latitude', 'longitude'],
                               v10=['time', 'latitude', 'longitude'],
                               t2m=['time', 'latitude', 'longitude'])

    ### define some basic variables ###
    nc_product.add_data(longitude=model_lon)
    nc_product.add_data(latitude=model_lat)

    ### read the model forecast output ###
    for i_time in range(len(time_range)):

        print(time_range[i_time])

        output = np.zeros([
            4, Config['Model'][model_scheme]['nlat'],
            Config['Model'][model_scheme]['nlon']
        ])

        for i_run in range(len(run_ids)):

            output += np.load(
                os.path.join(data_dir, run_ids[i_run], 'output_surface.npy'))

        output = output / len(run_ids)

        ### save the results to netcdf file ###
        nc_product = NcProduct(os.path.join(data_dir, 'surface.nc'), mode='a')
        nc_product.add_data(
            time=time_range[i_time].strftime('%Y-%m-%d %H:%M:%S'))

        nc_product.add_data(count=i_time, msl=output[0])
        nc_product.add_data(count=i_time, u10=output[1])
        nc_product.add_data(count=i_time, v10=output[2])
        nc_product.add_data(count=i_time, t2m=output[3])

        nc_product.close()

# end of product 1
####################################################################
