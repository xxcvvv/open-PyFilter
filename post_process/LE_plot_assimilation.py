'''
Autor: Mijie Pang
Date: 2023-04-22 19:20:07
LastEditTime: 2024-01-10 19:21:05
Description: 
'''
import os
import sys
import argparse
import numpy as np
import pandas as pd
import netCDF4 as nc
from typing import List
import multiprocessing as mp
from datetime import datetime

import post_lib as pol

sys.path.append('../')
from system_lib import read_json, read_json_dict
from tool.china_map import my_map as mmp
from tool.metrics import MetricTwoD

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Config = read_json_dict(config_dir, get_all=True)
Status = read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']
assi_scheme = Config['Assimilation']['scheme']['name']

parser = argparse.ArgumentParser()
parser.add_argument('-assimilation_time',
                    default=Status['assimilation']['assimilation_time'])
args = parser.parse_args()

assimilation_time = datetime.strptime(args.assimilation_time,
                                      '%Y-%m-%d %H:%M:%S')

model_lon = np.arange(Config['Model'][model_scheme]['start_lon'],
                      Config['Model'][model_scheme]['end_lon'],
                      Config['Model'][model_scheme]['res_lon'])
model_lat = np.arange(Config['Model'][model_scheme]['start_lat'],
                      Config['Model'][model_scheme]['end_lat'],
                      Config['Model'][model_scheme]['res_lat'])

results_path = os.path.join(
    Config['Info']['path']['output_path'],
    Config['Model'][model_scheme]['run_project'],
    Config['Assimilation'][assi_scheme]['project_name'], 'analysis',
    assimilation_time.strftime('%Y%m%d_%H%M'))

save_variables_dir = os.path.join(
    Config['Info']['path']['output_path'],
    Config['Model'][model_scheme]['run_project'],
    Config['Assimilation'][assi_scheme]['project_name'], 'analysis',
    assimilation_time.strftime('%Y%m%d_%H%M'))

extent = [80, 132, 15, 53]

if not os.path.exists(results_path):
    os.makedirs(results_path)


##################################################
###                plot results                ###
class PlotAssimilation:

    def __init__(self) -> None:

        self.methods = {
            'bc_pm10': self.plot_bc_pm10,
            'modis_dod': self.plot_modis,
            'viirs_dod': self.plot_viirs,
            'prior_dust_sfc': self.prior_dust_sfc,
            'prior_aod': self.prior_aod,
            'posterior_dust_sfc': self.posterior_dust_sfc,
            'posterior_aod': self.posterior_aod
        }

    def portal(self, method: str, *args, **kwargs) -> None:

        if method not in self.methods.keys():
            print(ValueError('Method ->"%s"<- is not supported.' % (method)))
        else:
            self.methods.get(method)(*args, **kwargs)

    ##############################################
    ###          get observation data          ###
    def get_bc_pm10(self, ) -> List:

        obs_flag = False
        if Config['Assimilation']['post_process']['with_observation']:

            # file_path = os.path.join(
            #     Config['Observation']['path'],
            #     Config['Observation']['bc_pm10']['dir_name'],
            #     'BC_PM10_%sUTC.csv' % (assimilation_time.strftime('%Y_%m_%d_%H%M')))

            file_path = os.path.join(
                Config['Observation']['path'],
                Config['Observation']['bc_pm10']['dir_name'],
                assimilation_time.strftime('%Y'), 'BC_PM10_%sUTC.csv' %
                (assimilation_time.strftime('%Y%m%d_%H%M')))

            if os.path.exists(file_path):
                obs_flag = True
                obs_data = pd.read_csv(file_path)
            else:
                obs_data = None

        return obs_flag, obs_data

    def get_modis(self, ) -> List:

        obs_flag = False
        if Config['Assimilation']['post_process']['with_observation']:

            file_path = os.path.join(
                Config['Observation']['path'],
                Config['Observation']['modis_dod']['dir_name'],
                assimilation_time.strftime('%Y'), 'MODIS_DOD_%sUTC.csv' %
                (assimilation_time.strftime('%Y%m%d_%H%M')))

            if os.path.exists(file_path):
                obs_flag = True
                obs_data = pd.read_csv(file_path)
                obs_data = obs_data[['Longitude', 'Latitude', 'DOD_550_DB_DT']]
            else:
                obs_data = None

        return obs_flag, obs_data

    def get_viirs(self, ) -> List:

        obs_flag = False
        if Config['Assimilation']['post_process']['with_observation']:

            file_path = os.path.join(
                Config['Observation']['path'],
                Config['Observation']['viirs_dod']['dir_name'],
                assimilation_time.strftime('%Y'), 'VIIRS_DOD_%sUTC.csv' %
                (assimilation_time.strftime('%Y%m%d_%H%M')))

            if os.path.exists(file_path):
                obs_flag = True
                obs_data = pd.read_csv(file_path)
                obs_data = obs_data[[
                    'Longitude', 'Latitude', 'DOD_550_Land_Ocean'
                ]]
            else:
                obs_data = None

        return obs_flag, obs_data

    ###################################################
    ### plot the specific data
    ### *--- Plot ---* ###
    def plot_bc_pm10(self, ):

        obs_flag, obs_data = self.get_bc_pm10()

        if obs_flag:

            obs_lon = obs_data.iloc[:, 0]
            obs_lat = obs_data.iloc[:, 1]
            obs_val = obs_data.iloc[:, 2]

            ### colorbar settings ###
            bounds = [0, 100, 300, 600, 1000, 2000, 3000, 4000, 1e9]
            colors = [
                '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA',
                '#1414FA', '#0000b3', '#000000'
            ]
            plot = mmp(quick=True,
                       map2=False,
                       title='BC-PM10 : %s' %
                       (assimilation_time.strftime('%Y-%m-%d %H:%M')),
                       bounds=bounds[:-1],
                       extent=extent,
                       colors=colors,
                       colorbar_shrink=0.6)
            plot.scatter(obs_lon, obs_lat, obs_val, meshgrid=False, size=48)
            plot.save('BC_PM10_' + assimilation_time.strftime('%Y%m%d_%H%M'),
                      path=results_path)
            plot.close()

    def plot_modis(self, ):

        obs_flag, obs_data = self.get_modis()

        if obs_flag:

            obs_lon = obs_data.iloc[:, 0]
            obs_lat = obs_data.iloc[:, 1]
            obs_val = obs_data.iloc[:, 2]

            ### colorbar settings ###
            bounds = [0, 0.1, 0.3, 0.6, 1, 2, 3, 4, 1e9]
            colors = [
                '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA',
                '#1414FA', '#0000b3', '#000000'
            ]
            plot = mmp(quick=True,
                       map2=False,
                       title='MODIS DOD : %s' %
                       (assimilation_time.strftime('%Y-%m-%d %H:%M')),
                       bounds=bounds[:-1],
                       extent=extent,
                       colors=colors,
                       colorbar_shrink=0.6)
            plot.scatter(obs_lon,
                         obs_lat,
                         obs_val,
                         edgecolor=None,
                         meshgrid=False,
                         size=24)
            plot.save('modis_dod_' + assimilation_time.strftime('%Y%m%d_%H%M'),
                      path=results_path)
            plot.close()

    def plot_viirs(self, ):

        obs_flag, obs_data = self.get_viirs()

        if obs_flag:

            obs_lon = obs_data.iloc[:, 0]
            obs_lat = obs_data.iloc[:, 1]
            obs_val = obs_data.iloc[:, 2]

            ### colorbar settings ###
            bounds = [0, 0.1, 0.3, 0.6, 1, 2, 3, 4, 1e9]
            colors = [
                '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA',
                '#1414FA', '#0000b3', '#000000'
            ]
            plot = mmp(quick=True,
                       map2=False,
                       title='VIIRS DOD : %s' %
                       (assimilation_time.strftime('%Y-%m-%d %H:%M')),
                       bounds=bounds[:-1],
                       extent=extent,
                       colors=colors,
                       colorbar_shrink=0.6)
            plot.scatter(obs_lon,
                         obs_lat,
                         obs_val,
                         edgecolor=None,
                         meshgrid=False,
                         size=24)
            plot.save('viirs_dod_' + assimilation_time.strftime('%Y%m%d_%H%M'),
                      path=results_path)
            plot.close()

    def prior_dust_sfc(self, *args, **kwargs) -> None:

        ### read the results ###
        try:
            if Config['Assimilation']['post_process']['save_method'] == 'npy':
                prior = np.load(save_variables_dir + '/prior.npy')

            elif Config['Assimilation']['post_process']['save_method'] == 'nc':

                sfc_file = save_variables_dir + '/prior_dust_sfc.nc'
                s3d_file = save_variables_dir + '/prior_dust_3d.nc'

                if os.path.exists(sfc_file):
                    with nc.Dataset(sfc_file) as nc_obj:
                        prior = nc_obj.variables['prior'][:]

                elif os.path.exists(s3d_file):
                    with nc.Dataset(s3d_file) as nc_obj:
                        prior = nc_obj.variables['prior'][0, :, :]

        except:
            return FileNotFoundError('Assimilation output file not found.')

        ### colorbar settings ###
        bounds = [0, 100, 300, 600, 1000, 2000, 3000, 4000, 1e9]
        colors = [
            '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
            '#0000b3', '#000000'
        ]

        obs_flag, obs_data = self.get_bc_pm10()
        obs_lon = obs_data.iloc[:, 0]
        obs_lat = obs_data.iloc[:, 1]
        obs_val = obs_data.iloc[:, 2]

        ### map the model state into observation space ###
        if obs_flag:
            mapped_obs = []
            mapped_prior = []
            count = 0
            for lon_o, lat_o in zip(obs_lon, obs_lat):
                map_lon = pol.find_nearest(lon_o, model_lon)
                map_lat = pol.find_nearest(lat_o, model_lat)
                if not np.isnan(map_lon) and not np.isnan(map_lat):
                    mapped_obs.append(obs_val[count])
                    mapped_prior.append(prior[map_lat, map_lon])
                count += 1

            metric = MetricTwoD(mapped_obs, mapped_prior)
            rmse = metric.calculate('rmse')
            nmb = metric.calculate('nmb')

        plot = mmp(quick=True,
                   map2=False,
                   title='%s-%s-prior : %s' %
                   (Config['Model'][model_scheme]['run_project'],
                    Config['Assimilation'][assi_scheme]['project_name'],
                    assimilation_time.strftime('%Y-%m-%d %H:%M')),
                   bounds=bounds[:-1],
                   extent=extent,
                   colors=colors,
                   colorbar_shrink=0.6)

        prior[prior <= 1e-9] = np.nan
        plot.contourf(model_lon, model_lat, prior, levels=bounds)
        if obs_flag:
            plot.scatter(obs_lon, obs_lat, obs_val, meshgrid=False, size=48)
            plot.text([0.37, 0.9], text='RMSE : %.1f' % (rmse))
            plot.text([0.37, 0.82], text=' NMB : %.1f ' % (nmb * 100) + '%')

        plot.save('dust_prior_sfc_snapshot_%s' %
                  (assimilation_time.strftime('%Y%m%d_%H%M')),
                  path=results_path)
        plot.close()

    def prior_aod(self, *args, **kwargs) -> None:

        ### read the results ###
        try:
            if Config['Assimilation']['post_process']['save_method'] == 'npy':
                prior = np.load(save_variables_dir + '/prior.npy')
            elif Config['Assimilation']['post_process']['save_method'] == 'nc':
                with nc.Dataset(save_variables_dir +
                                '/prior_aod.nc') as nc_obj:
                    prior = nc_obj.variables['prior'][:]

        except:
            return FileNotFoundError('Assimilation output file not found.')

        ### colorbar settings ###
        bounds = [0, 0.1, 0.3, 0.6, 1, 2, 3, 4, 1e9]
        colors = [
            '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
            '#0000b3', '#000000'
        ]

        obs_flag, obs_data = self.get_modis()

        ### map the model state into observation space ###
        if obs_flag:

            obs_lon = obs_data.iloc[:, 0]
            obs_lat = obs_data.iloc[:, 1]
            obs_val = obs_data.iloc[:, 2]

            mapped_obs = []
            mapped_posterior = []
            count = 0
            for lon_o, lat_o in zip(obs_lon, obs_lat):
                map_lon = pol.find_nearest(lon_o, model_lon)
                map_lat = pol.find_nearest(lat_o, model_lat)
                if not np.isnan(map_lon) and not np.isnan(map_lat):
                    mapped_obs.append(obs_val[count])
                    mapped_posterior.append(prior[map_lat, map_lon])
                count += 1

            metric = MetricTwoD(mapped_obs, mapped_posterior)
            rmse = metric.calculate('rmse')
            nmb = metric.calculate('nmb')

        plot = mmp(quick=True,
                   map2=False,
                   title='%s-%s-prior : %s' %
                   (Config['Model'][model_scheme]['run_project'],
                    Config['Assimilation'][assi_scheme]['project_name'],
                    assimilation_time.strftime('%Y-%m-%d %H:%M')),
                   bounds=bounds[:-1],
                   extent=extent,
                   colors=colors,
                   colorbar_shrink=0.6)

        prior[prior <= 1e-9] = np.nan
        plot.contourf(model_lon, model_lat, prior, levels=bounds)
        if obs_flag:
            # plot.scatter(obs_lon, obs_lat, obs_val, meshgrid=False, size=48)
            plot.text([0.37, 0.9], text='RMSE : %.1f' % (rmse))
            plot.text([0.37, 0.82], text=' NMB : %.1f ' % (nmb * 100) + '%')

        plot.save('aod_prior_snapshot_%s' %
                  (assimilation_time.strftime('%Y%m%d_%H%M')),
                  path=results_path)
        plot.close()

    def posterior_dust_sfc(self, *args, **kwargs) -> None:

        ### read the results ###
        try:
            if Config['Assimilation']['post_process']['save_method'] == 'npy':
                posterior = np.load(save_variables_dir + '/posterior.npy')

            elif Config['Assimilation']['post_process']['save_method'] == 'nc':

                sfc_file = save_variables_dir + '/posterior_dust_sfc.nc'
                s3d_file = save_variables_dir + '/posterior_dust_3d.nc'

                if os.path.exists(sfc_file):
                    with nc.Dataset(sfc_file) as nc_obj:
                        posterior = nc_obj.variables['posterior'][:]

                elif os.path.exists(s3d_file):
                    with nc.Dataset(s3d_file) as nc_obj:
                        posterior = nc_obj.variables['posterior'][0, :, :]

        except:
            return FileNotFoundError('Assimilation output file not found.')

        ### colorbar settings ###
        bounds = [0, 100, 300, 600, 1000, 2000, 3000, 4000, 1e9]
        colors = [
            '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
            '#0000b3', '#000000'
        ]

        obs_flag, obs_data = self.get_bc_pm10()

        ### map the model state into observation space ###
        if obs_flag:

            obs_lon = obs_data.iloc[:, 0]
            obs_lat = obs_data.iloc[:, 1]
            obs_val = obs_data.iloc[:, 2]

            mapped_obs = []
            mapped_posterior = []
            count = 0
            for lon_o, lat_o in zip(obs_lon, obs_lat):
                map_lon = pol.find_nearest(lon_o, model_lon)
                map_lat = pol.find_nearest(lat_o, model_lat)
                if not np.isnan(map_lon) and not np.isnan(map_lat):
                    mapped_obs.append(obs_val[count])
                    mapped_posterior.append(posterior[map_lat, map_lon])
                count += 1

            metric = MetricTwoD(mapped_obs, mapped_posterior)
            rmse = metric.calculate('rmse')
            nmb = metric.calculate('nmb')

        plot = mmp(quick=True,
                   map2=False,
                   title='%s-%s-posterior : %s' %
                   (Config['Model'][model_scheme]['run_project'],
                    Config['Assimilation'][assi_scheme]['project_name'],
                    assimilation_time.strftime('%Y-%m-%d %H:%M')),
                   bounds=bounds[:-1],
                   extent=extent,
                   colors=colors,
                   colorbar_shrink=0.6)

        posterior[posterior <= 1e-9] = np.nan
        plot.contourf(model_lon, model_lat, posterior, levels=bounds)
        if obs_flag:
            plot.scatter(obs_lon, obs_lat, obs_val, meshgrid=False, size=48)
            plot.text([0.37, 0.9], text='RMSE : %.1f' % (rmse))
            plot.text([0.37, 0.82], text=' NMB : %.1f ' % (nmb * 100) + '%')

        plot.save('dust_posterior_snapshot_%s' %
                  (assimilation_time.strftime('%Y%m%d_%H%M')),
                  path=results_path)
        plot.close()

    def posterior_aod(self, *args, **kwargs) -> None:

        ### read the results ###
        try:
            if Config['Assimilation']['post_process']['save_method'] == 'npy':
                posterior = np.load(save_variables_dir + '/posterior.npy')
            elif Config['Assimilation']['post_process']['save_method'] == 'nc':
                with nc.Dataset(save_variables_dir +
                                '/posterior_aod.nc') as nc_obj:
                    posterior = nc_obj.variables['posterior'][:]
        except:
            return FileNotFoundError('Assimilation output file not found.')

        ### colorbar settings ###
        bounds = [0, 0.1, 0.3, 0.6, 1, 2, 3, 4, 1e9]
        colors = [
            '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
            '#0000b3', '#000000'
        ]

        obs_flag, obs_data = self.get_modis()

        ### map the model state into observation space ###
        if obs_flag:

            obs_lon = obs_data.iloc[:, 0]
            obs_lat = obs_data.iloc[:, 1]
            obs_val = obs_data.iloc[:, 2]

            mapped_obs = []
            mapped_posterior = []
            count = 0
            for lon_o, lat_o in zip(obs_lon, obs_lat):
                map_lon = pol.find_nearest(lon_o, model_lon)
                map_lat = pol.find_nearest(lat_o, model_lat)
                if not np.isnan(map_lon) and not np.isnan(map_lat):
                    mapped_obs.append(obs_val[count])
                    mapped_posterior.append(posterior[map_lat, map_lon])
                count += 1

            metric = MetricTwoD(mapped_obs, mapped_posterior)
            rmse = metric.calculate('rmse')
            nmb = metric.calculate('nmb')

        plot = mmp(quick=True,
                   map2=False,
                   title='%s-%s-posterior : %s' %
                   (Config['Model'][model_scheme]['run_project'],
                    Config['Assimilation'][assi_scheme]['project_name'],
                    assimilation_time.strftime('%Y-%m-%d %H:%M')),
                   bounds=bounds[:-1],
                   extent=extent,
                   colors=colors,
                   colorbar_shrink=0.6)

        posterior[posterior <= 1e-9] = np.nan
        plot.contourf(model_lon, model_lat, posterior, levels=bounds)
        if obs_flag:
            # plot.scatter(obs_lon, obs_lat, obs_val, meshgrid=False, size=48)
            plot.text([0.37, 0.9], text='RMSE : %.1f' % (rmse))
            plot.text([0.37, 0.82], text=' NMB : %.1f ' % (nmb * 100) + '%')

        plot.save('aod_posterior_snapshot_%s' %
                  (assimilation_time.strftime('%Y%m%d_%H%M')),
                  path=results_path)
        plot.close()


### multi process the tasks ###
if Config['Assimilation']['post_process']['plot_results']:

    pool = mp.Pool(8)  # create a process pool
    results = []
    job_list = [
        'bc_pm10', 'modis_dod', 'viirs_dod', 'prior_dust_sfc', 'prior_aod',
        'posterior_dust_sfc', 'posterior_aod'
    ]  # all the products

    pa = PlotAssimilation()
    for i_job in range(len(job_list)):
        results.append(pool.apply_async(pa.portal, args=(job_list[i_job], )))

    results = [p.get() for p in results]
