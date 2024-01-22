'''
Autor: Mijie Pang
Date: 2023-10-23 20:55:00
LastEditTime: 2023-12-27 09:20:21
Description: 
'''
import os
import sys
import numpy as np

sys.path.append('../')

from tool.pack import NcProduct


class Output:

    def __init__(self, output_dir: str, config: dict, **Config) -> None:

        model_lon = np.arange(config['start_lon'], config['end_lon'],
                              config['res_lon'])
        model_lat = np.arange(config['start_lat'], config['end_lat'],
                              config['res_lat'])
        self.methods = {
            'prior_dust_3d': self.prior_dust_3d,
            'prior_dust_sfc': self.prior_dust_sfc,
            'prior_aod': self.prior_aod,
            'posterior_dust_3d': self.posterior_dust_3d,
            'posterior_dust_sfc': self.posterior_dust_sfc,
            'posterior_aod': self.posterior_aod
        }
        self.model_lon = model_lon
        self.model_lat = model_lat
        self.config = config
        self.Config = Config
        self.output_dir = output_dir

    ###########################################################
    ### method portals for saving output
    ### *--- Method Portal ---* ###
    def save(self, method: str, *args) -> None:

        if method not in self.methods.keys():
            raise ValueError('Invalid product name -> "%s" <-' % (method))

        self.methods.get(method)(*args)

    ###########################################################
    ### secific output products to save
    ### *--- Output Methods ---* ###
    ### save the dust prior in 3D space ###
    def prior_dust_3d(self, *args) -> None:

        x_f_3d = args[0]
        altitude = args[1]
        x_f_3d = self.kill_negative(x_f_3d)

        ### initiate the nc file ###
        nc_product = NcProduct(os.path.join(self.output_dir,
                                            'prior_dust_3d.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'],
                                    level=self.config['nlevel'])
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            altitude=['f4', ('level', 'latitude', 'longitude')],
            prior=['f4', ('level', 'latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(altitude=altitude.reshape(
            [self.config['nlevel'], self.config['nlat'], self.config['nlon']]))
        nc_product.add_data(prior=x_f_3d.reshape(
            [self.config['nlevel'], self.config['nlat'], self.config['nlon']]))

        nc_product.close()

    ### save the prior dust on surface ###
    def prior_dust_sfc(self, *args) -> None:

        x_f_sfc = args[0]
        x_f_sfc = self.kill_negative(x_f_sfc)

        ### initiate the nc file ###
        nc_product = NcProduct(os.path.join(self.output_dir,
                                            'prior_dust_sfc.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'])
        nc_product.define_variable(longitude=['f4', 'longitude'],
                                   latitude=['f4', 'latitude'],
                                   prior=['f4', ('latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(
            prior=x_f_sfc.reshape([self.config['nlat'], self.config['nlon']]))

        nc_product.close()

    ### save the priori aod mean ###
    def prior_aod(self, *args) -> None:

        x_f_mean = args[0]
        x_f_mean = self.kill_negative(x_f_mean)

        ### initiate the nc file ###
        nc_product = NcProduct(os.path.join(self.output_dir, 'prior_aod.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'])
        nc_product.define_variable(longitude=['f4', 'longitude'],
                                   latitude=['f4', 'latitude'],
                                   prior=['f4', ('latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(
            prior=x_f_mean.reshape([self.config['nlat'], self.config['nlon']]))

        nc_product.close()

    ### save the dust posterior on ground ###
    def posterior_dust_3d(self, *args) -> None:

        x_a_3d = args[0]
        altitude = args[1]
        x_a_3d = self.kill_negative(x_a_3d)

        ### initiate the nc file ###
        nc_product = NcProduct(os.path.join(self.output_dir,
                                            'posterior_dust_3d.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'],
                                    level=self.config['nlevel'])
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            level=['f4', 'level'],
            posterior=['f4', ('level', 'latitude', 'longitude')],
            altitude=['f4', ('level', 'latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(altitude=altitude)
        nc_product.add_data(posterior=x_a_3d.reshape(
            [self.config['nlevel'], self.config['nlat'], self.config['nlon']]))

        nc_product.close()

    ### save the poserior dust on surface ###
    def posterior_dust_sfc(self, *args) -> None:

        x_a_sfc = args[0]
        x_a_sfc = self.kill_negative(x_a_sfc)

        ### initiate the nc file ###
        nc_product = NcProduct(os.path.join(self.output_dir,
                                            'posterior_dust_sfc.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'])
        nc_product.define_variable(longitude=['f4', 'longitude'],
                                   latitude=['f4', 'latitude'],
                                   posterior=['f4', ('latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)

        nc_product.add_data(posterior=x_a_sfc.reshape(
            [self.config['nlat'], self.config['nlon']]))

        nc_product.close()

    ### save the posterior aod mean ###
    def posterior_aod(self, *args) -> None:

        x_a_mean = args[0]
        x_a_mean = self.kill_negative(x_a_mean)

        ### initiate the nc file ###
        nc_product = NcProduct(os.path.join(self.output_dir,
                                            'posteriori_aod.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'])
        nc_product.define_variable(longitude=['f4', 'longitude'],
                                   latitude=['f4', 'latitude'],
                                   posterior=['f4', ('latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(posterior=x_a_mean.reshape(
            [self.config['nlat'], self.config['nlon']]))

        nc_product.close()

    ###########################################################
    ### some useful functions
    ### *--- Functions ---* ###
    ### kill the negative and nan values ###
    def kill_negative(self, data: np.ndarray, fill_value=1e-9) -> np.ndarray:

        data[data < 0] = 0
        data[np.isnan(data)] = fill_value

        return data
