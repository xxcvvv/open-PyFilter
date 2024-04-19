'''
Autor: Mijie Pang
Date: 2024-03-28 20:39:59
LastEditTime: 2024-04-06 09:44:47
Description: 
'''
import os
import sys
import logging
import numpy as np
from datetime import datetime

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
from tool.china_map_v2 import MyMap as mmp
from tool.metrics import MetricTwoD
from tool.mapper import find_nearest_vector


class PlotAssimilation:

    def __init__(self,
                 Config: dict,
                 Status: dict,
                 extent=[80, 132, 15, 53]) -> None:

        model_scheme = Config['Model']['scheme']['name']
        assimilation_scheme = Config['Assimilation']['scheme']['name']
        assimilation_time = datetime.strptime(
            Status['assimilation']['assimilation_time'], '%Y-%m-%d %H:%M:%S')

        model_lon = np.arange(Config['Model'][model_scheme]['start_lon'],
                              Config['Model'][model_scheme]['end_lon'],
                              Config['Model'][model_scheme]['res_lon'])
        model_lat = np.arange(Config['Model'][model_scheme]['start_lat'],
                              Config['Model'][model_scheme]['end_lat'],
                              Config['Model'][model_scheme]['res_lat'])

        output_dir = os.path.join(
            Config['Info']['path']['output_path'],
            Config['Model'][model_scheme]['run_project'],
            Config['Assimilation'][assimilation_scheme]['project_name'],
            'analysis', assimilation_time.strftime('%Y%m%d_%H%M'))

        self.extent = extent
        self.output_dir = output_dir
        self.assimilation_time = assimilation_time
        self.model_lon = model_lon
        self.model_lat = model_lat
        self.model_scheme = model_scheme
        self.assimilation_scheme = assimilation_scheme
        self.Config = Config

    def pm10_only(self,
                  obs_data: None,
                  data_type: list,
                  bounds=[0, 100, 300, 600, 1000, 2000, 3000, 4000],
                  colors=[
                      '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA',
                      '#1414FA', '#0000b3', '#000000'
                  ],
                  extent=None):

        if obs_data is None:
            return
        else:
            plot = mmp(title='%s : %s' %
                       (data_type[0],
                        self.assimilation_time.strftime('%Y-%m-%d %H:%M')),
                       bounds=bounds,
                       extent=extent or self.extent,
                       colors=colors)
            plot.scatter(obs_data.iloc[:, 0],
                         obs_data.iloc[:, 1],
                         obs_data.iloc[:, 2],
                         meshgrid=False,
                         size=24)
            plot.save(
                os.path.join(
                    self.output_dir, '%s_%s.png' %
                    (data_type[1],
                     self.assimilation_time.strftime('%Y%m%d_%H%M'))))
            plot.close()

    def aod_only(self,
                 obs_data: None,
                 data_type: list,
                 bounds=[0, 0.1, 0.3, 0.6, 1, 2, 3, 4],
                 colors=[
                     '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA',
                     '#1414FA', '#0000b3', '#000000'
                 ],
                 extent=None):

        if obs_data is None:
            return
        else:
            plot = mmp(title='%s : %s' %
                       (data_type[0],
                        self.assimilation_time.strftime('%Y-%m-%d %H:%M')),
                       bounds=bounds,
                       extent=extent or self.extent,
                       colors=colors)
            plot.scatter(obs_data.iloc[:, 0],
                         obs_data.iloc[:, 1],
                         obs_data.iloc[:, 2],
                         meshgrid=False,
                         size=20,
                         edgecolor=None)
            plot.save(
                os.path.join(
                    self.output_dir, '%s_%s.png' %
                    (data_type[1],
                     self.assimilation_time.strftime('%Y%m%d_%H%M'))))
            plot.close()

    def contour_with_scatter(self,
                             data: np.ndarray,
                             data_type: list,
                             obs_data=None,
                             bounds=[0, 100, 300, 600, 1000, 2000, 3000, 4000],
                             colors=[
                                 '#F0F0F0', '#F0F096', '#FA9600', '#FA0064',
                                 '#9632FA', '#1414FA', '#0000b3', '#000000'
                             ],
                             extent=None):

        plot = mmp(
            map2=False,
            title='%s %s %s : %s' %
            (self.Config['Model'][self.model_scheme]['run_project'], self.
             Config['Assimilation'][self.assimilation_scheme]['project_name'],
             data_type[0], self.assimilation_time.strftime('%Y-%m-%d %H:%M')),
            bounds=bounds,
            extent=extent or self.extent,
            colors=colors)

        if not obs_data is None:
            lon_idxs = find_nearest_vector(obs_data.iloc[:, 0], self.model_lon)
            lat_idxs = find_nearest_vector(obs_data.iloc[:, 1], self.model_lat)
            data_mapped = data[lat_idxs, lon_idxs]
            metric = MetricTwoD(obs_data.iloc[:, 2], data_mapped)
            rmse = metric.calculate('rmse')
            nmb = metric.calculate('nmb')

        data[data <= 1e-9] = np.nan
        plot.contourf(self.model_lon, self.model_lat, data)

        if not obs_data is None:
            plot.scatter(obs_data.iloc[:, 0],
                         obs_data.iloc[:, 1],
                         obs_data.iloc[:, 2],
                         meshgrid=False,
                         size=24)
            plot.text(0.37, 0.9, f'RMSE : {rmse:.2f}')
            plot.text(0.37, 0.82, f' NMB : {(nmb * 100):.2f} %')

        plot.save(
            os.path.join(
                self.output_dir, '%s_%s.png' %
                (data_type[1],
                 self.assimilation_time.strftime('%Y%m%d_%H%M'))))
        plot.close()

    def aod_with_scatter(self,
                         data: np.ndarray,
                         data_type: list,
                         obs_data=None,
                         bounds=[0, 0.1, 0.3, 0.6, 1, 2, 3, 4],
                         colors=[
                             '#F0F0F0', '#F0F096', '#FA9600', '#FA0064',
                             '#9632FA', '#1414FA', '#0000b3', '#000000'
                         ],
                         extent=None):

        plot = mmp(
            map2=False,
            title='%s %s %s : %s' %
            (self.Config['Model'][self.model_scheme]['run_project'], self.
             Config['Assimilation'][self.assimilation_scheme]['project_name'],
             data_type[0], self.assimilation_time.strftime('%Y-%m-%d %H:%M')),
            bounds=bounds,
            extent=extent or self.extent,
            colors=colors)

        if not obs_data is None:
            lon_idxs = find_nearest_vector(obs_data.iloc[:, 0], self.model_lon)
            lat_idxs = find_nearest_vector(obs_data.iloc[:, 1], self.model_lat)
            data_mapped = data[lat_idxs, lon_idxs]
            metric = MetricTwoD(obs_data.iloc[:, 2], data_mapped)
            rmse = metric.calculate('rmse')
            nmb = metric.calculate('nmb')

        data[data <= 1e-9] = np.nan
        plot.contourf(self.model_lon, self.model_lat, data)

        if not obs_data is None:
            plot.scatter(obs_data.iloc[:, 0],
                         obs_data.iloc[:, 1],
                         obs_data.iloc[:, 2],
                         meshgrid=False,
                         size=24)
            plot.text(0.37, 0.9, f'RMSE : {rmse:.2f}')
            plot.text(0.37, 0.82, f' NMB : {(nmb * 100):.2f} %')

        plot.save(
            os.path.join(
                self.output_dir, '%s_%s.png' %
                (data_type[1],
                 self.assimilation_time.strftime('%Y%m%d_%H%M'))))
        plot.close()
