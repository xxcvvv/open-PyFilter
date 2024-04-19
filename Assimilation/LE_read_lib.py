'''
Autor: Mijie Pang
Date: 2023-10-27 19:58:33
LastEditTime: 2024-04-11 19:21:43
Description: 
'''
import os
import numpy as np
import netCDF4 as nc


class ModelReader:

    def __init__(self, model_dir: str, run_project=None) -> None:

        self.model_dir = model_dir
        self.run_project = run_project

    ### *--- Read the model simulations ---* ###

    def read_restart(self,
                     run_id: str,
                     time: None,
                     run_project=None,
                     var_name='c',
                     restart_dir=None,
                     time_format='%Y%m%d_%H%M') -> np.ndarray:
        """
        Read the model restart files.

        params:
            run_id: run identifier.
            time: restart file time.
            run_project: run project identifier.
            var_name: variable name to read.
            restart_dir: directory containing the restart files.
            time_format: used to format the time string.
        
        return:
            a numpy array containing the model simulation data.
        """

        if restart_dir is None:
            restart_dir = os.path.join(self.model_dir, run_project
                                       or self.run_project, run_id, 'restart')

        time_str = time.strftime(time_format)
        path = os.path.join(restart_dir, f'LE_{run_id}_state_{time_str}.nc')

        with nc.Dataset(path) as nc_obj:
            data = nc_obj.variables[var_name][:]

        return data

    def read_output(self,
                    run_id: str,
                    output_name: str,
                    time: None,
                    var_name: str,
                    run_project=None,
                    output_dir=None,
                    factor=1) -> np.ndarray:
        """
        Read the model output files.

        params:
            run_id: str, the unique identifier of the run.
            output_name: str, the name of the output.
            time: None, the time point of the data to be read.
            var_name: str, the name of the variable to be read.
            run_project: Optional[str], the name of the project to which the run belongs.
            output_dir: Optional[str], the directory where the output files are located.

        return:
            a numpy array containing the model simulation data.
        """

        if output_dir is None:
            output_dir = os.path.join(self.model_dir, run_project
                                      or self.run_project, run_id, 'output')

        path = os.path.join(
            output_dir,
            'LE_%s_%s_%s.nc' % (run_id, output_name, time.strftime('%Y%m%d')))

        with nc.Dataset(path) as nc_obj:

            output_time = nc_obj.variables['time']
            output_time = nc.num2date(output_time[:], output_time.units)
            output_time = [
                str(output_time[i_time]) for i_time in range(len(output_time))
            ]
            time_idx = output_time.index(time.strftime('%Y-%m-%d %H:%M:%S'))

            data = nc_obj.variables[var_name][time_idx, :] * factor

        return data


if __name__ == '__main__':

    mr = ModelReader()
    mr.read_output()
    mr.read_restart()
