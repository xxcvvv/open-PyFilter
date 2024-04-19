'''
Autor: Mijie Pang
Date: 2023-04-22 19:27:07
LastEditTime: 2024-04-17 19:22:14
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
from tool.china_map import my_map as mmp
from tool.mapper import find_nearest_vector


class PlotModelRun:

    def __init__(self, Config: dict, **kwargs):

        ### *--- initiate parameters ---* ###
        model_scheme = Config['Model']['scheme']['name']
        assimilation_scheme = Config['Assimilation']['scheme']['name']

        self.Ne = Config['Model'][model_scheme]['ensemble_number']
        self.nlon = Config['Model'][model_scheme]['nlon']
        self.nlat = Config['Model'][model_scheme]['nlat']

        self.model_lon = np.arange(Config['Model'][model_scheme]['start_lon'],
                                   Config['Model'][model_scheme]['end_lon'],
                                   Config['Model'][model_scheme]['res_lon'])
        self.model_lat = np.arange(Config['Model'][model_scheme]['start_lat'],
                                   Config['Model'][model_scheme]['end_lat'],
                                   Config['Model'][model_scheme]['res_lat'])
        self.iteration_num = Config['Model'][model_scheme]['iteration_num']

        # run_project = 'ChinaDust20210328_v22'
        # start_time = datetime.strptime('2021-03-27 01:00:00', '%Y-%m-%d %H:%M:%S')
        # end_time = datetime.strptime('2021-03-29 23:00:00', '%Y-%m-%d %H:%M:%S')

        self.run_project = Config['Model'][model_scheme]['run_project']
        self.model_output_dir = os.path.join(
            Config['Info']['path']['output_path'], self.run_project,
            'model_run')
        self.output_dir = os.path.join(Config['Info']['path']['output_path'],
                                       self.run_project, 'model_run',
                                       'snapshot', 'conc-sfc')

        self.spec = ['dust_ff', 'dust_f', 'dust_c', 'dust_cc', 'dust_ccc']
        self.bounds = [0, 100, 300, 600, 1000, 2000, 3000, 4000, 1e9]
        self.colors = [
            '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
            '#0000b3', '#000000'
        ]
        self.extent = [85, 135, 30, 50]

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def run(self, plot_time: datetime, data_type='restart', plot_pic=True):

        ### *--- read model simulation results ---* ###
        output = np.zeros([self.Ne, len(self.model_lat), len(self.model_lon)])
        model_flag = False
        if data_type == 'restart':

            for i_ensem in range(self.Ne):

                run_id = 'iter_%02d_ensem_%02d' % (self.iteration_num, i_ensem)
                path = os.path.join(
                    self.model_output_dir, run_id, 'restart',
                    'LE_%s_state_%s.nc' %
                    (run_id, plot_time.strftime('%Y%m%d_%H%M')))

                if os.path.exists(path):
                    model_flag = True

                    with nc.Dataset(path) as nc_obj:

                        for i_spec in range(len(self.spec)):
                            output[i_ensem, :, :] += nc_obj.variables['c'][
                                i_spec, 0, :, :]

        elif data_type == 'output':

            for i_ensem in range(self.Ne):

                run_id = 'iter_%02d_ensem_%02d' % (self.iteration_num, i_ensem)
                path = os.path.join(
                    self.model_output_dir, run_id, 'output',
                    'LE_%s_conc-3d_%s.nc' %
                    (run_id, plot_time.strftime('%Y%m%d')))

                if os.path.exists(path):
                    model_flag = True

                    with nc.Dataset(path) as nc_obj:

                        time = nc_obj.variables['time']
                        time = nc.num2date(time[:], time.units)
                        time = [
                            str(time[i_time]) for i_time in range(len(time))
                        ]
                        idx = time.index(
                            plot_time.strftime('%Y-%m-%d %H:%M:%S'))

                        for i_spec in range(len(self.spec)):
                            output[i_ensem, :, :] += nc_obj.variables[
                                self.spec[i_spec]][idx, 0, :, :] * (10**9)

        if model_flag:

            ### *----------------------------* ###
            ### *---   read observation   ---* ###
            output_mean = np.mean(output, axis=0)
            obs_flag = False
            obs_path = os.path.join(
                Config['Assimilation']['path']['observation_path'],
                Config['Assimilation']['observation']['data_type'],
                'BC_PM10_%sUTC.csv' % (plot_time.strftime('%Y_%m_%d_%H%M')))
            if os.path.exists(obs_path):

                obs_flag = True

                obs_data = pd.read_csv(obs_path, header=None)
                obs_lon = obs_data.iloc[:, 0].values
                obs_lat = obs_data.iloc[:, 1].values
                obs_val = obs_data.iloc[:, 2].values

                map_lon_idx = find_nearest_vector(obs_lon,
                                                  self.model_lon,
                                                  thres=0.1)
                map_lat_idx = find_nearest_vector(obs_lat,
                                                  self.model_lat,
                                                  thres=0.1)
                flag = np.logical_and(map_lon_idx >= 0, map_lat_idx >= 0)
                map_lon_idx, map_lat_idx = map_lon_idx[flag], map_lat_idx[flag]

                mapped_obs = obs_val[flag]
                mapped_simu = output_mean[map_lat_idx, map_lon_idx]
                metric = MetricTwoD(mapped_obs, mapped_simu)
                rmse = metric.calculate('rmse')
                nmb = metric.calculate('nmb')

            ### *--------------------------* ###
            ### *---      for plot      ---* ###
            if plot_pic:

                plot = mmp(quick=False,
                           map2=False,
                           title='Model run : ' +
                           plot_time.strftime('%Y-%m-%d %H:%M'),
                           extent=self.extent,
                           bounds=self.bounds[:-1],
                           colors=self.colors,
                           colorbar_shrink=0.6)

                output_mean[output_mean < 1e-6] = np.nan
                plot.contourf(self.model_lon,
                              self.model_lat,
                              output_mean,
                              levels=self.bounds)

                if obs_flag:
                    plot.scatter(obs_lon,
                                 obs_lat,
                                 obs_val,
                                 meshgrid=False,
                                 size=12)
                    plot.text([0.37, 0.9], text='RMSE : %.1f' % (rmse, 1))
                    plot.text([0.37, 0.82],
                              text=' NMB : %.1f' % (nmb * 100) + '%')

                plot.save(plot_time.strftime('%Y%m%d_%H%M'),
                          path=self.output_dir)
                plot.close()

            if obs_flag:
                return [plot_time.strftime('%Y%m%d_%H%M'), rmse, nmb]
            else:
                return [plot_time.strftime('%Y%m%d_%H%M'), np.nan, np.nan]


def main(Config: dict, **kwargs):

    start_time = datetime.strptime(
        Config['Initial']['initial_run']['start_time'], '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(Config['Initial']['initial_run']['end_time'],
                                 '%Y-%m-%d %H:%M:%S')
    time_range = pd.date_range(start_time, end_time, freq='1H')

    start_t = datetime.now()
    num_cores = 12
    pool = mp.Pool(num_cores)

    pmr = PlotModelRun(Config, **kwargs)
    results = [pool.apply_async(pmr.run, args=(time, )) for time in time_range]
    results = [p.get() for p in results]

    # metric_data = pd.DataFrame(results)
    # metric_data.columns = ['Time', 'RMSEs', 'NMBs']

    # metric_data[['Time', 'RMSEs']].to_csv(output_dir + '/metrics_rmse.csv',
    #                                       index=None)
    # metric_data[['Time', 'NMBs']].to_csv(output_dir + '/metrics_nmb.csv',
    #                                      index=None)

    end_t = datetime.now()
    elapsed_sec = (end_t - start_t).total_seconds()
    print('process time : %.2f s' % (elapsed_sec))


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    main(Config)
