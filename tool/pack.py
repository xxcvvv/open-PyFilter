'''
Autor: Mijie Pang
Date: 2023-09-14 21:07:10
LastEditTime: 2024-01-10 15:56:55
Description: 
'''
import os
import gc
import pickle
import numpy as np
import netCDF4 as nc
from datetime import datetime


### a class designed for creating nc files ###
class NcProduct:

    def __init__(self,
                 file_path='test.nc',
                 mode='w',
                 format='NETCDF4',
                 **kwarg):

        if not file_path.endswith('.nc'):
            file_path = file_path + '.nc'

        nc_file = nc.Dataset(file_path, mode, format=format)

        ### create a new netcdf file ###
        if mode == 'w':

            nc_file.Create_time = 'Created at %s UTC' % (
                datetime.now().utcnow().strftime('%Y-%m-%d %H:%M:%S'))

            ### define some global attributions ###
            if 'Model' in kwarg.keys() and 'Assimilation' in kwarg.keys():

                model_scheme = kwarg['Model']['scheme']['name']
                assimilation_scheme = kwarg['Assimilation']['scheme']['name']
                nc_file.Project = '%s -> %s' % (
                    kwarg['Model'][model_scheme]['run_project'],
                    kwarg['Assimilation'][assimilation_scheme]['project_name'])

            if 'Info' in kwarg.keys():

                nc_file.Producer = kwarg['Info']['System']['name']
                nc_file.version = str(kwarg['Info']['System']['version'])
                nc_file.Author = kwarg['Info']['User']['Author']
                nc_file.Institute = kwarg['Info']['User']['Institute']
                nc_file.Host = kwarg['Info']['Machine']['Host']

        ### append to an existing netcdf file ###
        if mode == 'a':

            variable_dict = {}
            for i_var in nc_file.variables:
                variable_dict[i_var] = nc_file.variables[i_var]

            self.variable_dict = variable_dict

        self.nc_file = nc_file

    ### define the dimension in the nc file ###
    def define_dimension(self, **dims):

        self.dimension_dict = {}
        for i_key in dims.keys():
            self.dimension_dict[i_key] = self.nc_file.createDimension(
                i_key, size=dims[i_key])

    ### define the variables and their dimension in the nc file and
    def define_variable(self, **kwarg):

        self.variable_dict = {}
        for i_key in kwarg.keys():
            self.variable_dict[i_key] = self.nc_file.createVariable(
                i_key, kwarg[i_key][0], dimensions=kwarg[i_key][1])

    def define_variable_dict(self, var_dict):

        for i_key in var_dict.keys():
            self.variable_dict[i_key] = self.nc_file.createVariable(
                i_key, var_dict[i_key][0], dimensions=var_dict[i_key][1])

    ### set global attribute ###
    def set_attr_global(self, **kwargs) -> None:

        for key in kwargs.keys():
            self.nc_file.setncattr(key, kwargs[key])

    ### set attribute to variables ###
    def set_attr(self, **kwargs) -> None:

        for var in kwargs.keys():
            for attr_name in kwargs[var].keys():
                self.nc_file.variables[var].setncattr(attr_name,
                                                      kwargs[var][attr_name])

    ### add the data to the variables ###
    def add_data(self, count=1e9, **kwarg) -> None:

        for key in kwarg.keys():

            ### determine if the variables contain a unlimited dimendion ###
            ### Attention： the first dimension must be time if there is unlimited dim ###
            unlimit_flag = False
            time_dim = str(self.variable_dict[key].dimensions[0])
            if self.nc_file.dimensions[time_dim].isunlimited():
                unlimit_flag = True

            ### add the data ###
            if unlimit_flag:
                time_dim_size = self.nc_file.dimensions[time_dim].size
                if count < 1e9:
                    self.variable_dict[key][count] = kwarg[key]
                    # print(count)
                else:
                    self.variable_dict[key][time_dim_size] = kwarg[key]
                    # print(time_dim_size)
                self.variable_dict[key][time_dim_size] = kwarg[key]
            else:
                self.variable_dict[key][:] = kwarg[key]

    def add_data_dict(self, data_dict: dict, count=1e9) -> None:

        for key in data_dict.keys():

            ### determine if the variables contain a unlimited dimendion ###
            ### Attention： the first dimension must be time if there is unlimited dim ###
            unlimit_flag = False
            time_dim = str(self.variable_dict[key].dimensions[0])
            if self.nc_file.dimensions[time_dim].isunlimited():
                unlimit_flag = True

            ### add the data ###
            if unlimit_flag:
                time_dim_size = self.nc_file.dimensions[time_dim].size
                if count < 1e9:
                    self.variable_dict[key][count] = data_dict[key]
                    # print(count)
                else:
                    self.variable_dict[key][time_dim_size] = data_dict[key]
                    # print(time_dim_size)
            else:
                self.variable_dict[key][:] = data_dict[key]

    ### return the nc file ###
    def return_obj(self):
        return self.nc_file

    ## close the nc file ###
    def close(self):

        self.nc_file.history = 'Last modified at %s UTC' % (
            datetime.now().utcnow().strftime('%Y-%m-%d %H:%M:%S'))
        self.nc_file.close()
        self.variable_dict = {}
        gc.collect()


### decorator : save function output to pickle format ###
def cache(path='.', name='test', format='pkl'):

    def decorator(func):

        def wrapper(*args, **kwargs):

            result = func(*args, **kwargs)

            if format == 'pkl':
                with open(
                        os.path.join(
                            path, name +
                            datetime.now().strftime('T%Y%m%d%H%M%S.pkl')),
                        "wb") as file:
                    pickle.dump(result, file)

            elif format == 'npy':
                np.save(
                    os.path.join(
                        path, name + datetime.now().strftime('T%Y%m%d%H%M%S')),
                    result)

            elif format == None:
                pass

            return result

        return wrapper

    return decorator


if __name__ == '__main__':

    nc_produce = NcProduct('test.nc')
    nc_produce.define_dimension(longitude=180, latitude=20, time=None)
    nc_produce.define_variable(lon=['f4', 'longitude'],
                               lat=['f4', 'latitude'],
                               time=['S1', 'time'],
                               data=['f4', ('time', 'longitude', 'latitude')])
    nc_produce.set_attr_global(Title='test',
                               Producer='PyFilter',
                               version='1.1')
    nc_produce.set_attr(data={'unit': 'm'},
                        lon={
                            'unit': 'test',
                            'angle': 'None'
                        })
    nc_produce.close()

    nc_produce = NcProduct('test.nc', mode='a')
    nc_produce.add_data(data=np.random.random([180, 20]))
    nc_produce.close()

    # @cache(path='.', name='test', format='npy')
    # @timer(count=True)
    def calculate_square(number):
        return number**2

    calculate_square(4)
