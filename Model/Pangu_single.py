'''
Autor: Mijie Pang
Date: 2023-08-19 17:03:45
LastEditTime: 2023-09-11 08:54:16
Description: 
'''
import os
import sys
import onnx
import numpy as np
import pandas as pd
import netCDF4 as nc
import onnxruntime as ort
from datetime import datetime

sys.path.append('../')
import system_lib as stl
from tool.pack import NcProduct

home_dir = os.getcwd()
pid = os.getpid()
total_start = datetime.now()

config_dir = '../config'
status_path = '../Status.json'

stl.edit_json(
    path=status_path,
    new_dict={'model': {
        'home_dir': home_dir,
        'code': 10,
        'pid': pid
    }})

### read system configuration ###
Config = stl.read_json_dict(config_dir, 'Model.json', 'Info.json',
                            'Assimilation.json', 'Input.json')
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

###################################################
###       Section : initialize parameters       ###
run_time_range = pd.date_range(
    Status['model']['start_time'],
    Status['model']['end_time'],
    freq=Config['Model'][model_scheme]['output_time_interval'])

system_log.debug('run time range : %s' % (run_time_range))

model_selected_path = os.path.join(
    Config['Model']['pangu']['path']['model_path'], 'pangu_weather_%s.onnx' %
    (Config['Model'][model_scheme]['output_time_interval'][:-1]))

output_dir = os.path.join(
    Config['Info']['path']['output_path'],
    Config['Model'][model_scheme]['run_project'],
    Config['Assimilation'][assimilation_scheme]['project_name'], 'forecast',
    run_time_range[0].strftime('%Y%m%d_%H%M'), 'output', 'basic')

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

#####################################################
###    Section : initialize the netcdf product    ###
surface_nc_path = os.path.join(
    Config['Info']['path']['output_path'],
    Config['Model'][model_scheme]['run_project'],
    Config['Assimilation'][assimilation_scheme]['project_name'], 'forecast',
    run_time_range[0].strftime('%Y%m%d_%H%M'), 'output_surface_UTC.nc')

nc_product = NcProduct(surface_nc_path,
                       Model=Config['Model'],
                       Assimilation=Config['Assimilation'],
                       Info=Config['Info'])

nc_product.define_dimension(longitude=Config['Model'][model_scheme]['nlon'],
                            latitude=Config['Model'][model_scheme]['nlat'],
                            time=None)

nc_product.define_variable(longitude=['f4', 'longitude'],
                           latitude=['f4', 'latitude'],
                           time=['S19', 'time'],
                           msl=['f4', ('time', 'latitude', 'longitude')],
                           u10=['f4', ('time', 'latitude', 'longitude')],
                           v10=['f4', ('time', 'latitude', 'longitude')],
                           t2m=['f4', ('time', 'latitude', 'longitude')])

### define some basic variables ###
model_lon = np.linspace(0.125, 359.875, 1440)
model_lat = np.linspace(90, -90, 721)
nc_product.add_data(longitude=model_lon)
nc_product.add_data(latitude=model_lat)

nc_product.close()
system_log.debug('nc file created')

###############################################
###      Section : read the input data      ###

### surface input ###
product_type = 'reanalysis-era5-single-levels'
input_surface = np.zeros((4, 721, 1440), dtype=np.float32)
for i_var, var in enumerate(
        Config['Input']['era5']['product'][product_type]['vars']):

    with nc.Dataset(
            os.path.join(
                Config['Input']['path'], product_type, 'ERA5_%s_%s.nc' %
                (var, run_time_range[0].strftime('%Y%m%d_%H%M')))) as nc_obj:

        input_surface[i_var] = nc_obj.variables[Config['Input']['era5'][
            'product'][product_type]['var_names'][i_var]][:].astype(np.float32)

system_log.info('Surface input collected')

###  upper data ###
product_type = 'reanalysis-era5-pressure-levels'
input_upper = np.zeros((5, 13, 721, 1440), dtype=np.float32)
for i_var, var in enumerate(
        Config['Input']['era5']['product'][product_type]['vars']):

    with nc.Dataset(
            os.path.join(
                Config['Input']['path'], product_type, 'ERA5_%s_%s.nc' %
                (var, run_time_range[0].strftime('%Y%m%d_%H%M')))) as nc_obj:

        input_upper[i_var] = nc_obj.variables[Config['Input']['era5'][
            'product'][product_type]['var_names'][i_var]][:].astype(np.float32)

system_log.info('Upper input collected')

################################################
###      Section : load the pangu model      ###
system_log.debug('loading the model')
pangu_model = onnx.load(model_selected_path)

### Set the behavier of onnxruntime ###
options = ort.SessionOptions()
options.enable_cpu_mem_arena = False
options.enable_mem_pattern = False
options.enable_mem_reuse = False

options.intra_op_num_threads = Config['Model']['node'][
    'core_demand']  # Increase the number for faster inference and more memory consumption
system_log.debug('Number of thread(s) : %s' %
                 (Config['Model']['node']['core_demand']))

### Initialize onnxruntime session for Pangu-Weather Models ###
### use GPU ###
if Config['Model']['node']['gpu']:
    # Set the behavier of cuda provider
    cuda_provider_options = {
        'arena_extend_strategy': 'kSameAsRequested',
    }
    ort_session = ort.InferenceSession(model_selected_path,
                                       sess_options=options,
                                       providers=[('CUDAExecutionProvider',
                                                   cuda_provider_options)])

    system_log.debug('Using the GPU')

### use CPU ###
else:
    ort_session = ort.InferenceSession(model_selected_path,
                                       sess_options=options,
                                       providers=['CPUExecutionProvider'])

    system_log.debug('Using the CPU')

system_log.debug('Pangu model path : %s' % (model_selected_path))
system_log.info('Pangu model is loaded')

####################################################
###     Section : loop the forecast sequence     ###

for i_time in range(len(run_time_range) - 1):

    system_log.info('%s forecast starts from %s -> %s' %
                    (Config['Model'][model_scheme]['output_time_interval'],
                     run_time_range[i_time].strftime('%Y-%m-%d %H:%M'),
                     run_time_range[i_time + 1].strftime('%Y-%m-%d %H:%M')))

    #################################################################
    ### The inference session
    start = datetime.now()

    input_upper, input_surface = ort_session.run(None, {
        'input': input_upper,
        'input_surface': input_surface
    })

    system_log.info('Inference took %.2f s' %
                    ((datetime.now() - start).total_seconds()))

    ### end of inference session
    #################################################################

    ### Save the results to npy files ###
    np.save(
        os.path.join(
            output_dir, 'output_upper_%s_%s' %
            (run_time_range[i_time].strftime('%Y%m%d_%H%M'),
             Config['Model'][model_scheme]['output_time_interval'])),
        input_upper)
    np.save(
        os.path.join(
            output_dir, 'output_surface_%s_%s' %
            (run_time_range[i_time].strftime('%Y%m%d_%H%M'),
             Config['Model'][model_scheme]['output_time_interval'])),
        input_surface)

    ### save the results to netcdf file ###
    nc_product = NcProduct(surface_nc_path, mode='a')
    nc_product.add_data(time=run_time_range[i_time +
                                            1].strftime('%Y-%m-%d %H:%M:%S'))

    nc_product.add_data_dict(
        {
            'msl': input_surface[0],
            'u10': input_surface[1],
            'v10': input_surface[2],
            't2m': input_surface[3]
        },
        count=i_time)
    nc_product.close()

    system_log.debug('forecast output packed to %s' % (surface_nc_path))

system_log.info('Forecast took %.2f s in total' %
                ((datetime.now() - total_start).total_seconds()))
