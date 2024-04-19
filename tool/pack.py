'''
Autor: Mijie Pang
Date: 2023-09-14 21:07:10
LastEditTime: 2024-04-07 10:27:22
Description: designed to store outputs in nc file
'''
import numpy as np
import netCDF4 as nc
from pytz import utc
from datetime import datetime


### *--- Designed for nc file creation ---* ###
class NcProduct:

    def __init__(self,
                 file_path='test.nc',
                 mode='w',
                 format='NETCDF4',
                 **kwargs):

        self.file_path = self.ensure_nc_extension(file_path)
        self.nc_file = nc.Dataset(self.file_path, mode, format=format)
        self.init_file(mode, **kwargs)

    @staticmethod
    def ensure_nc_extension(file_path: str) -> str:

        return file_path if file_path.endswith('.nc') else f'{file_path}.nc'

    ### *--- initiate the nc file ---* ###
    def init_file(self, mode: str, **kwargs) -> None:

        if mode == 'w':
            self.set_creation_time()
            self.set_global_attributes(**kwargs)

    def set_creation_time(self) -> None:

        self.nc_file.setncattr(
            'Create_time',
            datetime.now().astimezone(utc).strftime('%Y-%m-%d %H:%M:%S UTC'))

    ### *--- define some global attributions ---* ###
    def set_global_attributes(self, **kwargs) -> None:

        if 'Model' in kwargs.keys() and 'Assimilation' in kwargs.keys():

            model_scheme = kwargs['Model']['scheme']['name']
            assimilation_scheme = kwargs['Assimilation']['scheme']['name']
            self.nc_file.Project = '%s -> %s' % (
                kwargs['Model'][model_scheme]['run_project'],
                kwargs['Assimilation'][assimilation_scheme]['project_name'])

        if 'Info' in kwargs.keys():

            self.nc_file.System = kwargs['Info']['System']['name']
            self.nc_file.version = str(kwargs['Info']['System']['version'])
            self.nc_file.User = kwargs['Info']['User']['Name']
            self.nc_file.Institute = kwargs['Info']['User']['Institute']
            self.nc_file.Host = kwargs['Info']['Machine']['Host']

    ### *--- define the dimension of the nc file ---* ###
    def define_dimension(self, **dims) -> None:

        for name, size in dims.items():
            self.nc_file.createDimension(name, size=size)

    ### *--- define the variables and their dimension in the nc file ---* ###
    def define_variable(self, **kwargs) -> None:

        for var_name, (var_type, dimensions) in kwargs.items():
            self.nc_file.createVariable(var_name, var_type, dimensions)

    def define_variable_dict(self, var_dict):

        for i_key in var_dict.keys():
            self.nc_file.createVariable(i_key,
                                        var_dict[i_key][0],
                                        dimensions=var_dict[i_key][1])

    ### *--- set global attribute ---* ###
    def set_attr_global(self, **kwargs) -> None:

        for key in kwargs.keys():
            self.nc_file.setncattr(key, kwargs[key])

    ### *--- set attribute of variables ---* ###
    def set_attr(self, **kwargs) -> None:

        for var in kwargs.keys():
            for attr_name in kwargs[var].keys():
                self.nc_file.variables[var].setncattr(attr_name,
                                                      kwargs[var][attr_name])

    ### *--- add the data ---* ###
    def add_data(self, count=1e9, **kwargs) -> None:

        for key in kwargs.keys():

            ### determine if the variables contain a unlimited dimendion ###
            ### Attention： the first dimension must be time if there is unlimited dim ###
            unlimit_flag = False
            time_dim = str(self.nc_file[key].dimensions[0])
            if self.nc_file.dimensions[time_dim].isunlimited():
                unlimit_flag = True

            ### *--- time dimension ---* ###
            if unlimit_flag:

                time_dim_size = self.nc_file.dimensions[time_dim].size
                # assign the dim to add the data
                if not count == 1e9:
                    self.nc_file[key][count] = kwargs[key]
                # append the data after the last time dim
                else:
                    self.nc_file[key][time_dim_size] = kwargs[key]

            ### *--- not time dimension ---* ###
            else:

                self.nc_file[key][:] = kwargs[key]

    ### *--- close the nc file ---* ###
    def close(self) -> None:

        self.nc_file.setncattr(
            'history', 'Last modified at %s' %
            datetime.now().astimezone(utc).strftime('%Y-%m-%d %H:%M:%S UTC'))
        self.nc_file.close()


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
    print(dir(nc_produce))
    nc_produce.close()

    nc_produce = NcProduct('test.nc', mode='a')
    nc_produce.add_data(data=np.random.random([180, 20]))
    nc_produce.close()

    nc_produce = NcProduct('test.nc', mode='a')
    nc_produce.add_data(data=np.random.random([180, 20]))
    nc_produce.close()
