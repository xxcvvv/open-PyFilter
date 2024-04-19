'''
Autor: Mijie Pang
Date: 2023-10-23 20:55:00
LastEditTime: 2024-04-11 19:22:31
Description: designed to generate the assimilation output files
'''
import os
import sys
import numpy as np

sys.path.append('../')

from tool.pack import NcProduct

### kill the negative and nan values ###
def kill_negative(data: np.ndarray, fill_value=1e-9) -> np.ndarray:

    data[data < 0] = 0
    data[np.isnan(data)] = fill_value

    return data


class Output:

    def __init__(self, output_dir: str, config: dict, **Config) -> None:

        model_lon = np.arange(config['start_lon'], config['end_lon'],
                              config['res_lon'])
        model_lat = np.arange(config['start_lat'], config['end_lat'],
                              config['res_lat'])
        self.methods = {
            'prior_dust': self.prior_dust,
            'prior_dust_3d': self.prior_dust_3d,
            'prior_dust_all': self.prior_dust_all,
            'prior_dust_sfc': self.prior_dust_sfc,
            'prior_aod': self.prior_aod,
            'posterior_dust': self.posterior_dust,
            'posterior_dust_3d': self.posterior_dust_3d,
            'posterior_dust_all': self.posterior_dust_all,
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
    def save(self, method: str, *args, **kwargs) -> None:

        if method not in self.methods.keys():
            raise ValueError('Invalid product name -> "%s" <-' % (method))

        self.methods.get(method)(*args, **kwargs)

    ###########################################################
    ### secific output products to save
    ### *--- Output Methods ---* ###

    ### *--- save the dust prior with level and bin ---* ###
    def prior_dust(self, *args, **kwargs) -> None:

        data = args[0]
        data = kill_negative(data)
        data = data.reshape([
            self.config['nspec'], self.config['nlevel'], self.config['nlat'],
            self.config['nlon']
        ])
        altitude = kwargs.get('altitude', None)
        std = kwargs.get('std', None)

        ### *--- initiate the nc file ---* ###
        nc_product = NcProduct(os.path.join(self.output_dir, 'prior_dust.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'],
                                    level=self.config['nlevel'],
                                    bin=self.config['nspec'])
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            prior=['f4', ('bin', 'level', 'latitude', 'longitude')])

        ### *-- add the data ---* ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(prior=data)

        if not altitude is None:
            nc_product.define_variable(
                altitude=['f4', ('level', 'latitude', 'longitude')])
            nc_product.add_data(altitude=altitude)

        if not std is None:
            nc_product.define_variable(
                prior_std=['f4', ('level', 'latitude', 'longitude')])
            nc_product.add_data(prior_std=std)

        nc_product.close()

    ### *--- save the dust prior in 3D space ---* ###
    def prior_dust_3d(self, *args, **kwargs) -> None:

        x_f_3d = args[0]
        x_f_3d = kill_negative(x_f_3d)

        altitude = kwargs.get('altitude', None)

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
            prior=['f4', ('level', 'latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(prior=x_f_3d.reshape(
            [self.config['nlevel'], self.config['nlat'], self.config['nlon']]))

        if not altitude is None:
            nc_product.define_variable(
                altitude=['f4', ('level', 'latitude', 'longitude')])
            nc_product.add_data(altitude=altitude)

        nc_product.close()

    ### *--- save the all the prior data ---* ###
    def prior_dust_all(self, *args, **kwargs) -> None:

        x_f_all = args[0]
        x_f_all = kill_negative(x_f_all)

        Ne = kwargs.get('Ne', self.config['ensemble_number'])

        x_f_all = x_f_all.reshape([
            Ne, self.config['nspec'], self.config['nlevel'],
            self.config['nlat'], self.config['nlon']
        ])

        ### initiate the nc file ###
        nc_product = NcProduct(os.path.join(self.output_dir,
                                            'prior_dust_all.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'],
                                    level=self.config['nlevel'],
                                    spec=self.config['nspec'],
                                    Ne=Ne)
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            prior=['f4', ('Ne', 'spec', 'level', 'latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(prior=x_f_all)

        nc_product.close()

    ### *--- save the prior dust on surface ---* ###
    def prior_dust_sfc(self, *args, **kwargs) -> None:

        x_f_sfc = args[0]
        x_f_sfc = kill_negative(x_f_sfc)

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

    ### *--- save the prior aod mean ---* ###
    def prior_aod(self, *args, **kwargs) -> None:

        x_f_mean = args[0]
        x_f_mean = kill_negative(x_f_mean)

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

    ### *--- save the dust posterior with level and bin ---* ###
    def posterior_dust(self, *args, **kwargs) -> None:

        data = args[0]
        data = kill_negative(data)
        data = data.reshape([
            self.config['nspec'], self.config['nlevel'], self.config['nlat'],
            self.config['nlon']
        ])
        altitude = kwargs.get('altitude', None)
        std = kwargs.get('std', None)

        ### *--- initiate the nc file ---* ###
        nc_product = NcProduct(os.path.join(self.output_dir,
                                            'posterior_dust.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'],
                                    level=self.config['nlevel'],
                                    bin=self.config['nspec'])
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            posterior=['f4', ('bin', 'level', 'latitude', 'longitude')])

        ### *-- add the data ---* ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(posterior=data)

        if not altitude is None:
            nc_product.define_variable(
                altitude=['f4', ('level', 'latitude', 'longitude')])
            nc_product.add_data(altitude=altitude)

        if not std is None:
            nc_product.define_variable(
                posterior_std=['f4', ('level', 'latitude', 'longitude')])
            nc_product.add_data(posterior_std=std)

        nc_product.close()

    ### *--- save the dust posterior on ground ---* ###
    def posterior_dust_3d(self, *args, **kwargs) -> None:

        x_a_3d = args[0]
        x_a_3d = kill_negative(x_a_3d)

        altitude = kwargs.get('altitude', None)

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
            posterior=['f4', ('level', 'latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(posterior=x_a_3d.reshape(
            [self.config['nlevel'], self.config['nlat'], self.config['nlon']]))

        if not altitude is None:
            nc_product.define_variable(
                altitude=['f4', ('level', 'latitude', 'longitude')])
            nc_product.add_data(altitude=altitude)

        nc_product.close()

    ### *--- save the dust posterior on ground ---* ###
    def posterior_dust_all(self, *args, **kwargs) -> None:

        x_a_all = args[0]
        x_a_all = kill_negative(x_a_all)

        Ne = kwargs.get('Ne', self.config['ensemble_number'])

        x_a_all = x_a_all.reshape([
            Ne, self.config['nspec'], self.config['nlevel'],
            self.config['nlat'], self.config['nlon']
        ])

        ### initiate the nc file ###
        nc_product = NcProduct(os.path.join(self.output_dir,
                                            'posterior_dust_all.nc'),
                               Model=self.Config['Model'],
                               Assimilation=self.Config['Assimilation'],
                               Info=self.Config['Info'])
        nc_product.define_dimension(longitude=self.config['nlon'],
                                    latitude=self.config['nlat'],
                                    level=self.config['nlevel'],
                                    bin=self.config['nspec'],
                                    Ne=Ne)
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            posterior=['f4', ('Ne', 'bin', 'level', 'latitude', 'longitude')])

        ### add data to the file ###
        nc_product.add_data(longitude=self.model_lon)
        nc_product.add_data(latitude=self.model_lat)
        nc_product.add_data(posterior=x_a_all)

        nc_product.close()

    ### *--- save the poserior dust on surface ---* ###
    def posterior_dust_sfc(self, *args, **kwargs) -> None:

        x_a_sfc = args[0]
        x_a_sfc = kill_negative(x_a_sfc)

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

    ### *--- save the posterior aod mean ---* ###
    def posterior_aod(self, *args, **kwargs) -> None:

        x_a_mean = args[0]
        x_a_mean = kill_negative(x_a_mean)

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
