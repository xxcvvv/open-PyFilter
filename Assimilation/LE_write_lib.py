'''
Autor: Mijie Pang
Date: 2023-10-23 20:55:33
LastEditTime: 2024-02-18 21:07:14
Description: 
'''
import os
import numpy as np
import netCDF4 as nc


class WriteRestart:

    def __init__(self, model_dir: str, run_project: str) -> None:

        self.model_dir = model_dir
        self.run_project = run_project

    def write(self, data: np.ndarray, run_id: str, time: None, var_name: str,
              **kwargs) -> None:

        if kwargs.get('screen', True):
            data = self.kill_negative(data)

        path = os.path.join(
            self.model_dir, self.run_project, run_id, 'restart',
            'LE_%s_state_%s.nc' % (run_id, time.strftime('%Y%m%d_%H%M')))

        with nc.Dataset(path, 'r+') as nc_obj:
            nc_obj.variables[var_name][:] = data[:]

    ######################################################
    ### Some useful functions
    ### *--- Functions ---* ###
    ### eliminate all the values that less then 0 ###
    def kill_negative(self, data: np.ndarray, fill_value=1e-9) -> np.ndarray:

        data[data <= 0] = fill_value
        data[np.isnan(data)] = fill_value

        return data

    ### convert 2d to 3d field ###
    def convert2full(self, data_2d: np.ndarray, mass_partition: np.ndarray,
                     spec_partition: np.ndarray, Nlon: int, Nlat: int,
                     Nlev: int, Nspec: int) -> np.ndarray:

        data_2d = data_2d.reshape([Nlat, Nlon])
        data_3d = np.zeros([Nspec, Nlev, Nlat, Nlon])

        for level in range(Nlev):
            for spec in range(Nspec):
                data_3d[spec, level, :, :] = spec_partition[level, spec] * (
                    data_2d[:, :] * mass_partition[level, :, :])

        data_3d = self.kill_negative(data_3d)

        return data_3d

    ### allocate the structure to the full space ###
    def allocate2full(self, data_2d: np.ndarray, ratio: np.ndarray, Nlon: int,
                      Nlat: int, Nlev: int, Nspec: int) -> np.ndarray:

        data_2d = data_2d.reshape([Nlat, Nlon])
        data_3d = np.zeros([Nspec, Nlev, Nlat, Nlon])

        data_3d = data_2d * ratio
        data_3d = self.kill_negative(data_3d)

        return data_3d


######################################################
### Some useful functions
### *--- Functions ---* ###
### eliminate all the values that less then 0 ###
def kill_negative(data: np.ndarray, fill_value=1e-9) -> np.ndarray:

    data[data <= 0] = fill_value
    data[np.isnan(data)] = fill_value

    return data


### convert 2d to 3d field ###
def convert2full(data_2d: np.ndarray, mass_partition: np.ndarray,
                 spec_partition: np.ndarray, Nlon: int, Nlat: int, Nlev: int,
                 Nspec: int) -> np.ndarray:

    data_2d = data_2d.reshape([Nlat, Nlon])
    data_3d = np.zeros([Nspec, Nlev, Nlat, Nlon])

    for level in range(Nlev):
        for spec in range(Nspec):
            data_3d[spec, level, :, :] = spec_partition[level, spec] * (
                data_2d[:, :] * mass_partition[level, :, :])

    data_3d = kill_negative(data_3d)

    return data_3d


### allocate the structure to the full space ###
def allocate2full(data_2d: np.ndarray, ratio: np.ndarray, Nlon: int, Nlat: int,
                  Nlev: int, Nspec: int) -> np.ndarray:

    data_2d = data_2d.reshape([Nlat, Nlon])
    data_3d = np.zeros([Nspec, Nlev, Nlat, Nlon])

    data_3d = data_2d * ratio
    data_3d = kill_negative(data_3d)

    return data_3d
