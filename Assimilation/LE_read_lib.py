'''
Autor: Mijie Pang
Date: 2023-10-27 19:58:33
LastEditTime: 2024-01-04 21:33:52
Description: 
'''
import os
import numpy as np
import netCDF4 as nc


class Model_Reader:

    def __init__(self, model_dir: str) -> None:

        self.model_dir = model_dir

    ### read the model simulations ###
    ### *--- Read ---* ###
    # read the model restart files
    def read_restart(self,
                     run_project: str,
                     run_id: str,
                     time: None,
                     var_name='c',
                     restart_dir=None) -> np.ndarray:

        if restart_dir is None:
            restart_dir = os.path.join(self.model_dir, run_project, run_id,
                                       'restart')

        path = os.path.join(
            restart_dir,
            'LE_%s_state_%s.nc' % (run_id, time.strftime('%Y%m%d_%H%M')))

        with nc.Dataset(path) as nc_obj:
            data = nc_obj.variables[var_name][:]

        return data

    # read the model output files
    def read_output(self,
                    output_name: str,
                    var_name: str,
                    time: None,
                    run_project='',
                    run_id='',
                    output_dir=None,
                    factor=1) -> np.ndarray:

        if output_dir is None:
            output_dir = os.path.join(self.model_dir, run_project, run_id,
                                      'output')

        path = os.path.join(
            output_dir,
            'LE_%s_%s_%s.nc' % (run_id, output_name, time.strftime('%Y%m%d')))

        ### open the model restart files ###
        with nc.Dataset(path) as nc_obj:

            output_time = nc_obj.variables['time']
            output_time = nc.num2date(output_time[:], output_time.units)
            output_time = [
                str(output_time[i_time]) for i_time in range(len(output_time))
            ]
            time_idx = output_time.index(time.strftime('%Y-%m-%d %H:%M:%S'))

            data = nc_obj.variables[var_name][time_idx, :] * factor

        return data
