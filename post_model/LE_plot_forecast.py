'''
Autor: Mijie Pang
Date: 2023-04-22 19:25:41
LastEditTime: 2024-04-15 16:40:08
Description: 
'''
import os
import sys
import numpy as np
import pandas as pd
import netCDF4 as nc
import multiprocessing as mp
from datetime import datetime

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
import system_lib as stl

from tool.metrics import MetricTwoD
from tool.china_map_v2 import MyMap as mmp
from tool.mapper import find_nearest_vector


class PlotForecast:

    def __init__(self, Config: dict, Status: dict) -> None:

        model_scheme = Config['Model']['scheme']['name']
        assimilation_scheme = Config['Assimilation']['scheme']['name']

        results_dir = os.path.join(
            Config['Info']['path']['output_path'],
            Config['Model'][model_scheme]['run_project'],
            Config['Assimilation'][assimilation_scheme]['project_name'])

        start_time = datetime.strptime(Status['model']['start_time'],
                                       '%Y-%m-%d %H:%M:%S')

        ### *---  make directories   ---* ###
        output_dir = os.path.join(results_dir, 'forecast',
                                  start_time.strftime('%Y%m%d_%H%M'))

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        self.Config = Config
        self.Status = Status
        self.model_scheme = model_scheme
        self.assimilation_scheme = assimilation_scheme
        self.output_dir = output_dir

    def worker(self, forecast_time: datetime):

        spec = ['dust_ff', 'dust_f', 'dust_c', 'dust_cc', 'dust_ccc']
        bounds = [0, 100, 300, 600, 1000, 2000, 3000, 4000, 1e9]
        colors = [
            '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
            '#0000b3', '#000000'
        ]
        extent = [85, 135, 30, 50]

        Ne = self.Config['Model'][self.model_scheme]['ensemble_number']
        model_lon = np.arange(
            self.Config['Model'][self.model_scheme]['start_lon'],
            self.Config['Model'][self.model_scheme]['end_lon'],
            self.Config['Model'][self.model_scheme]['res_lon'])
        model_lat = np.arange(
            self.Config['Model'][self.model_scheme]['start_lat'],
            self.Config['Model'][self.model_scheme]['end_lat'],
            self.Config['Model'][self.model_scheme]['res_lat'])
        iteration_num = self.Config['Model'][
            self.model_scheme]['iteration_num']

        ### *----------------------------* ###
        ### *---   Get model output   ---* ###
        if self.Config['Model'][self.model_scheme]['run_type'] == 'ensemble':

            output = np.zeros([Ne, len(model_lat), len(model_lon)])

            for i in range(Ne):

                run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i)
                path = os.path.join(
                    self.output_dir, 'forecast_files', run_id, 'output',
                    'LE_%s_conc-sfc_%s.nc' %
                    (run_id, forecast_time.strftime('%Y%m%d')))

                with nc.Dataset(path) as nc_obj:

                    time = nc_obj.variables['time']
                    time = nc.num2date(time[:], time.units)
                    time = [str(time[i_time]) for i_time in range(len(time))]
                    idx = time.index(
                        forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

                    for j in range(len(spec)):
                        output[i, :, :] += nc_obj.variables[spec[j]][
                            idx, 0, :, :] * (10**9)

            output_mean = np.mean(output, axis=0)

        elif self.Config['Model'][self.model_scheme]['run_type'] == 'single':

            output_mean = np.zeros([len(model_lat), len(model_lon)])

            run_id = self.Config['Assimilation'][
                self.assimilation_scheme]['project_name']
            path = os.path.join(
                self.output_dir, 'forecast_files', 'output',
                'LE_%s_conc-3d_%s.nc' %
                (run_id, forecast_time.strftime('%Y%m%d')))

            with nc.Dataset(path) as nc_obj:

                time = nc_obj.variables['time']
                time = nc.num2date(time[:], time.units)
                time = [str(time[i_time]) for i_time in range(len(time))]
                idx = time.index(forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

                for j in range(len(spec)):
                    output_mean[:, :] += nc_obj.variables[spec[j]][
                        idx, 0, :, :] * (10**9)

        elif self.Config['Model'][
                self.model_scheme]['run_type'] == 'ensemble_extend':

            time_set = self.Config['Assimilation'][
                self.assimilation_scheme]['time_set']
            ensemble_set = self.Config['Assimilation'][
                self.assimilation_scheme]['ensemble_set']
            ensemble_set = [
                int(ensemble_set[i_ensem])
                for i_ensem in range(len(ensemble_set))
            ]
            Ne_extend = sum(ensemble_set)
            run_id = [[
                't_' + str(time_set[i_time]) + '_e_%02d' % (i_ensem)
                for i_ensem in range(ensemble_set[i_time])
            ] for i_time in range(len(time_set))]

            output = np.zeros([Ne_extend, len(model_lat), len(model_lon)])

            for i_time in range(len(time_set)):
                for i_ensem in range(ensemble_set[i_time]):

                    Ne_count = sum(ensemble_set[:i_time]) + i_ensem

                    path = os.path.join(
                        self.output_dir, 'forecast_files',
                        run_id[i_time][i_ensem], 'output',
                        'LE_%s_conc-3d_%s.nc' %
                        (run_id[i_time][i_ensem],
                         forecast_time.strftime('%Y%m%d')))

                    with nc.Dataset(path) as nc_obj:

                        time = nc_obj.variables['time']
                        time = nc.num2date(time[:], time.units)
                        time = [
                            str(time[i_time]) for i_time in range(len(time))
                        ]
                        idx = time.index(
                            forecast_time.strftime('%Y-%m-%d %H:%M:%S'))

                        for j in range(len(spec)):
                            output[Ne_count, :, :] += nc_obj.variables[
                                spec[j]][idx, 0, :, :] * (10**9)

            output_mean = np.mean(output, axis=0)

        ### *----------------------------------* ###
        ### *---      Get observation      --- * ###
        # get observation data and calculate the metrics
        obs_flag = False
        file_path = os.path.join(
            self.Config['Observation']['path'],
            self.Config['Observation']['bc_pm10']['dir'],
            'BC_PM10_%sUTC.csv' % (forecast_time.strftime('%Y_%m_%d_%H%M')))

        if os.path.exists(file_path):

            obs_flag = True

            obs_data = pd.read_csv(file_path, header=None)
            obs_lon = obs_data.iloc[:, 0]
            obs_lat = obs_data.iloc[:, 1]
            obs_val = obs_data.iloc[:, 2]

            map_lon_idx = find_nearest_vector(obs_lon, model_lon, thres=0.1)
            map_lat_idx = find_nearest_vector(obs_lat, model_lat, thres=0.1)
            flag = np.logical_and(map_lon_idx >= 0, map_lat_idx >= 0)
            map_lon_idx, map_lat_idx = map_lon_idx[flag], map_lat_idx[flag]

            mapped_obs=obs_val[flag]
            mapped_simu=output_mean[map_lat_idx, map_lon_idx]
            metric = MetricTwoD(mapped_obs, mapped_simu)
            rmse = metric.calculate('rmse')
            nmb = metric.calculate('nmb')

        ### *--------------------------* ###
        ### *---      for plot      ---* ###
        plot = mmp(title=forecast_time.strftime('%Y%m%d_%H%M'),
                   bounds=bounds,
                   extent=extent,
                   colors=colors)

        output_mean[output_mean <= 1e-9] = np.nan
        plot.contourf(model_lon, model_lat, output_mean)

        if obs_flag:
            plot.text(0.37, 0.9, f'RMSE : {rmse:.2f}')
            plot.text(0.37, 0.82, f' NMB : {(nmb*100):.2f} %')

            if self.Config['Model'][
                    self.model_scheme]['post_process']['with_observation']:
                plot.scatter(obs_lon,
                             obs_lat,
                             obs_val,
                             meshgrid=False,
                             size=48)

        plot.save(
            os.path.join(self.output_dir,
                         forecast_time.strftime('%Y%m%d_%H%M.png')))
        plot.close()

        if obs_flag:
            return [forecast_time.strftime('%Y%m%d_%H%M'), rmse]
        else:
            return [forecast_time.strftime('%Y%m%d_%H%M'), np.nan]


def main(Config: dict, Status: dict):

    model_scheme = Config['Model']['scheme']['name']
    start_time = datetime.strptime(Status['model']['start_time'],
                                   '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(Status['model']['end_time'],
                                 '%Y-%m-%d %H:%M:%S')
    time_range = pd.date_range(
        start_time,
        end_time,
        freq=Config['Model'][model_scheme]['output_time_interval'])

    pf = PlotForecast(Config, Status)

    ### *-------------------------------* ###
    ### *---   parallel processing   ---* ###
    start_t = datetime.now()

    num_cores = Config['Model'][model_scheme]['post_process']['run_spec'][1]
    pool = mp.Pool(num_cores)
    results = [
        pool.apply_async(pf.worker, args=(forecast_time, ))
        for forecast_time in time_range
    ]
    results = [p.get() for p in results]

    end_t = datetime.now()
    elapsed_sec = (end_t - start_t).total_seconds()
    print('process time : {:.2f} s'.format(elapsed_sec))


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    Status = stl.read_json(path=os.path.join(main_dir, 'Status.json'))
    main(Config, Status)
