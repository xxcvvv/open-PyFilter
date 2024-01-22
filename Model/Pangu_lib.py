'''
Autor: Mijie Pang
Date: 2023-08-19 17:58:52
LastEditTime: 2023-11-03 09:12:44
Description: 
'''
import os
import sys
import onnx
import numpy as np
import netCDF4 as nc
import onnxruntime as ort
from datetime import datetime

sys.path.append('../')
import system_lib as stl
from tool.pack import NcProduct


def run_full(run_id='',
             model_path='',
             Config={},
             run_time_range=None,
             system_log=None,
             **kwargs) -> None:

    ### initial parameters ###
    if 'info' in kwargs.keys():

        info = kwargs['info']
        run_id = info['run_id']
        model_path = info['model_path']
        Config = info['Config']
        Status = info['Status']
        run_time_range = info['run_time_range']

        system_log = stl.Logging(os.path.join(
            Status['system']['home_dir'],
            Status['system']['system_project'] + '.log'),
                                 level=Config['Info']['System']['log_level'])

    model_scheme = Config['Model']['scheme']['name']
    assimilation_scheme = Config['Assimilation']['scheme']['name']

    system_log.info('%s started' % (run_id))

    ### load the model ###
    pangu = pangu_model(model_path,
                        threads=Config['Model']['node']['core_demand'],
                        run_time_range=run_time_range,
                        Config=Config,
                        log=system_log)

    output_dir = os.path.join(
        Config['Info']['path']['output_path'],
        Config['Model'][model_scheme]['run_project'],
        Config['Assimilation'][assimilation_scheme]['project_name'],
        'forecast', run_time_range[0].strftime('%Y%m%d_%H%M'), 'output',
        run_id)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    ### initiate the output nc file ###
    pangu.init_surface_nc(os.path.join(output_dir, 'output_surface_UTC.nc'))
    pangu.init_upper_nc(os.path.join(output_dir, 'output_upper_UTC.nc'))

    ### read the initial era reanalysis data ###
    input_surface, input_upper = pangu.read_era5()

    ### loop over the forecast time range ###
    for i_time in range(len(run_time_range) - 1):

        system_log.info(
            '%s forecast starts from %s -> %s' %
            (Config['Model'][model_scheme]['output_time_interval'],
             run_time_range[i_time].strftime('%Y-%m-%d %H:%M'),
             run_time_range[i_time + 1].strftime('%Y-%m-%d %H:%M')))

        ### model inference session ###
        pangu.inference(input_upper=input_upper, input_surface=input_surface)

        ### pack the output to nc files ###
        pangu.pack_surface(time=run_time_range[i_time + 1], counter=i_time)
        pangu.pack_upper(time=run_time_range[i_time + 1], counter=i_time)

        ### assign the output as the input in next time step ###
        input_surface = pangu.output_surface
        input_upper = pangu.output_upper

    system_log.info('%s finished' % (run_id))


class model_class:

    def __init__(self,
                 model_path: str,
                 Config={},
                 run_time_range=None,
                 system_log=None,
                 **kwargs) -> None:

        ### initiate parameters ###
        if 'info' in kwargs.keys():

            info = kwargs['info']
            model_path = info['model_path']
            Config = info['Config']
            Status = info['Status']
            run_time_range = info['run_time_range']

            system_log = stl.Logging(
                os.path.join(Status['system']['home_dir'],
                             Status['system']['system_project'] + '.log'),
                level=Config['Info']['System']['log_level'])

        self.pangu = pangu_model(
            model_path,
            threads=Config['Model']['node']['core_demand'],
            run_time_range=run_time_range,
            Config=Config,
            log=system_log)

        self.Config = Config
        self.system_log = system_log
        self.run_time_range = run_time_range
        self.model_scheme = Config['Model']['scheme']['name']
        self.assimilation_scheme = Config['Assimilation']['scheme']['name']

    def run(self, run_id: str):

        output_dir = os.path.join(
            self.Config['Info']['path']['output_path'],
            self.Config['Model'][self.model_scheme]['run_project'],
            self.Config['Assimilation'][
                self.assimilation_scheme]['project_name'], 'forecast',
            self.run_time_range[0].strftime('%Y%m%d_%H%M'), 'output', run_id)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        ### initiate the output nc file ###
        self.pangu.init_surface_nc(
            os.path.join(output_dir, 'output_surface_UTC.nc'))
        self.pangu.init_upper_nc(
            os.path.join(output_dir, 'output_upper_UTC.nc'))

        ### read the initial era reanalysis data ###
        input_surface, input_upper = self.pangu.read_era5()

        ### loop over the forecast time range ###
        for i_time in range(len(self.run_time_range) - 1):

            self.system_log.info(
                '%s forecast starts from %s -> %s' %
                (self.Config['Model'][
                    self.model_scheme]['output_time_interval'],
                 self.run_time_range[i_time].strftime('%Y-%m-%d %H:%M'),
                 self.run_time_range[i_time + 1].strftime('%Y-%m-%d %H:%M')))

            ### model inference session ###
            self.pangu.inference(input_upper=input_upper,
                                 input_surface=input_surface)

            ### pack the output to nc files ###
            self.pangu.pack_surface(time=self.run_time_range[i_time + 1],
                                    counter=i_time)
            self.pangu.pack_upper(time=self.run_time_range[i_time + 1],
                                  counter=i_time)

            ### assign the output as the input in next time step ###
            input_surface = self.pangu.output_surface
            input_upper = self.pangu.output_upper

        self.system_log.info('%s finished' % (run_id))


class pangu_model:

    def __init__(self,
                 model_path: str,
                 run_time_range: any,
                 Config: dict,
                 use_gpu=False,
                 threads=4,
                 log=None) -> None:

        log.debug('loading model from %s' % (model_path))

        pangu_model = onnx.load(model_path)

        ### Set the behavier of onnxruntime ###
        options = ort.SessionOptions()
        options.enable_cpu_mem_arena = False
        options.enable_mem_pattern = False
        options.enable_mem_reuse = False

        options.intra_op_num_threads = threads  # Increase the number for faster inference and more memory consumption

        ### use GPU ###
        if use_gpu:

            # Set the behavier of cuda provider
            cuda_provider_options = {
                'arena_extend_strategy': 'kSameAsRequested',
            }
            ort_session = ort.InferenceSession(model_path,
                                               sess_options=options,
                                               providers=[
                                                   ('CUDAExecutionProvider',
                                                    cuda_provider_options)
                                               ])

            log.debug('Using the GPU')

        ### use CPU ###
        else:
            ort_session = ort.InferenceSession(
                model_path,
                sess_options=options,
                providers=['CPUExecutionProvider'])

            log.debug('Using the CPU')

        log.debug('model loaded')

        self.ort_session = ort_session
        self.Config = Config
        self.run_time_range = run_time_range
        self.log = log

    def read_era5(self):

        ### surface input ###
        product_type = 'reanalysis-era5-single-levels'
        input_surface = np.zeros((4, 721, 1440), dtype=np.float32)
        for i_var, var in enumerate(
                self.Config['Input']['era5']['product'][product_type]['vars']):

            with nc.Dataset(
                    os.path.join(
                        self.Config['Input']['path'], product_type,
                        'ERA5_%s_%s.nc' %
                        (var, self.run_time_range[0].strftime('%Y%m%d_%H%M')))
            ) as nc_obj:

                input_surface[i_var] = nc_obj.variables[
                    self.Config['Input']['era5']['product'][product_type]
                    ['var_names'][i_var]][:].astype(np.float32)

        self.log.debug('Surface input collected')

        ###  upper data ###
        product_type = 'reanalysis-era5-pressure-levels'
        input_upper = np.zeros((5, 13, 721, 1440), dtype=np.float32)
        for i_var, var in enumerate(
                self.Config['Input']['era5']['product'][product_type]['vars']):

            with nc.Dataset(
                    os.path.join(
                        self.Config['Input']['path'], product_type,
                        'ERA5_%s_%s.nc' %
                        (var, self.run_time_range[0].strftime('%Y%m%d_%H%M')))
            ) as nc_obj:

                input_upper[i_var] = nc_obj.variables[
                    self.Config['Input']['era5']['product'][product_type]
                    ['var_names'][i_var]][:].astype(np.float32)

        self.log.debug('Upper input collected')

        return input_surface, input_upper

    def inference(self, input_upper, input_surface):

        start = datetime.now()

        output_upper, output_surface = self.ort_session.run(
            None, {
                'input': input_upper,
                'input_surface': input_surface
            })

        self.log.info('Inference took %.2f s' %
                      ((datetime.now() - start).total_seconds()))

        self.output_upper = output_upper
        self.output_surface = output_surface

    ### create the empty surface output nc file ###
    def init_surface_nc(self, nc_path: str) -> None:

        nc_product = NcProduct(nc_path,
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])

        model_scheme = self.Config['Model']['scheme']['name']

        nc_product.define_dimension(
            longitude=self.Config['Model'][model_scheme]['nlon'],
            latitude=self.Config['Model'][model_scheme]['nlat'],
            time=None)

        nc_product.define_variable(
            longitude=['f4', 'longitude'],
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
        self.log.debug('surface output file created')

        self.surface_nc_path = nc_path

    ### save the surface output ###
    def pack_surface(self, time: datetime, counter: int) -> None:

        ### save the results to netcdf file ###
        nc_product = NcProduct(self.surface_nc_path, mode='a')
        nc_product.add_data(time=time.strftime('%Y-%m-%d %H:%M:%S'))

        nc_product.add_data_dict(
            {
                'msl': self.output_surface[0],
                'u10': self.output_surface[1],
                'v10': self.output_surface[2],
                't2m': self.output_surface[3]
            },
            count=counter)
        nc_product.close()

        self.log.debug('forecast output packed to %s' % (self.surface_nc_path))

    ### create the empty upper output nc file ###
    def init_upper_nc(self, nc_path: str) -> None:

        nc_product = NcProduct(nc_path,
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])

        model_scheme = self.Config['Model']['scheme']['name']

        nc_product.define_dimension(
            longitude=self.Config['Model'][model_scheme]['nlon'],
            latitude=self.Config['Model'][model_scheme]['nlat'],
            level=self.Config['Model'][model_scheme]['nlevel'],
            time=None)

        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            level=['i4', 'level'],
            time=['S19', 'time'],
            z=['f4', ('time', 'level', 'latitude', 'longitude')],
            q=['f4', ('time', 'level', 'latitude', 'longitude')],
            t=['f4', ('time', 'level', 'latitude', 'longitude')],
            u=['f4', ('time', 'level', 'latitude', 'longitude')],
            v=['f4', ('time', 'level', 'latitude', 'longitude')])

        ### define some basic variables ###
        model_lon = np.linspace(0.125, 359.875, 1440)
        model_lat = np.linspace(90, -90, 721)
        model_level = self.Config['Input']['era5']['product'][
            'reanalysis-era5-pressure-levels']['pressure_level']
        model_level = [int(level) for level in model_level]

        nc_product.add_data(longitude=model_lon)
        nc_product.add_data(latitude=model_lat)
        nc_product.add_data(level=model_level)

        nc_product.close()
        self.log.debug('upper output file created')

        self.upper_nc_path = nc_path

    ### save the upper output ###
    def pack_upper(self, time: datetime, counter: int):

        ### save the results to netcdf file ###
        nc_product = NcProduct(self.upper_nc_path, mode='a')
        nc_product.add_data(time=time.strftime('%Y-%m-%d %H:%M:%S'))

        nc_product.add_data_dict(
            {
                'z': self.output_upper[0],
                'q': self.output_upper[1],
                't': self.output_upper[2],
                'u': self.output_upper[3],
                'v': self.output_upper[4]
            },
            count=counter)
        nc_product.close()

        self.log.debug('forecast output packed to %s' % (self.upper_nc_path))
