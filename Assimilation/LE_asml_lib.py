'''
Autor: Mijie Pang
Date: 2023-05-28 16:09:30
LastEditTime: 2023-12-17 15:14:10
Description: library designed for lotos-euros assimilation 
'''
import os
import numpy as np
import netCDF4 as nc


def dist_calculation(lon_dist: np.ndarray, lat_dist: np.ndarray,
                     distance_threshold: int) -> float:

    distance = ((lon_dist * 100)**2 + (lat_dist * 100)**2)**0.5
    s = distance / distance_threshold

    if s < 1:
        correlation = 1 - 5 / 3 * s**2 + 5 / 8 * s**3 + 1 / 2 * s**4 - 1 / 4 * s**5
    else:
        if s < 2:
            correlation = -2 / 3 * s**(-1) + 4 - 5 * s + 5 / 3 * s**2 + \
                 5 / 8 * s**3 - 1 / 2 * s**4 + 1 / 12 * s**5
        else:
            correlation = 0

    return correlation


def localization(simu_lon: np.ndarray, simu_lat: np.ndarray,
                 distance_threshold: int) -> np.ndarray:

    simu_lon, simu_lat = np.meshgrid(simu_lon, simu_lat)
    simu_lon = np.ravel(simu_lon)
    simu_lat = np.ravel(simu_lat)
    Ns = len(simu_lon)
    L = np.zeros([Ns, Ns])

    for i in range(Ns):
        for j in range(i, Ns):
            r = dist_calculation(simu_lon[i] - simu_lon[j],
                                 simu_lat[i] - simu_lat[j], distance_threshold)
            L[i, j] = r
            L[j, i] = r

    return L


### base version of write the new restart file ###
def write_new_nc(posteriori: np.ndarray,
                 restart_path: str,
                 run_id: str,
                 time=None) -> None:

    posteriori[posteriori <= 0] = 0e-9
    posteriori[np.isnan(posteriori)] = 0e-9

    path = os.path.join(
        restart_path, run_id, 'restart',
        'LE_%s_state_%s.nc' % (run_id, time.strftime('%Y%m%d_%H%M')))
    with nc.Dataset(path, 'r+') as nc_obj:
        nc_obj.variables['c'][:] = posteriori[:]

    # print('written : ' + path)


def write_new_nc_patch(info: list) -> None:

    posteriori, restart_path, run_id, time = info
    posteriori[posteriori <= 0] = 0e-9
    posteriori[np.isnan(posteriori)] = 0e-9

    path = os.path.join(
        restart_path, run_id, 'restart',
        'LE_%s_state_%s.nc' % (run_id, time.strftime('%Y%m%d_%H%M')))

    with nc.Dataset(path, 'r+') as nc_obj:
        nc_obj.variables['c'][:] = posteriori[:]

    # print('written : ' + path)


### write new restart files ###
def write_new_nc_ensemble(posteriori: np.ndarray,
                          Ne: int,
                          restart_path: str,
                          time: None,
                          iteration_num=0) -> None:

    posteriori[posteriori <= 0] = 0e-9
    posteriori[np.isnan(posteriori)] = 0e-9

    for i_ensem in range(Ne):

        run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)

        path = os.path.join(
            restart_path, run_id, 'restart',
            'LE_%s_state_%s.nc' % (run_id, time.strftime('%Y%m%d_%H%M')))

        with nc.Dataset(path, 'r+') as nc_obj:

            nc_obj.variables['c'][:] = posteriori[i_ensem]


def write_new_nc_single(posteriori: np.ndarray, restart_path: str, run_id: str,
                        time: None) -> None:

    posteriori[posteriori <= 0] = 0e-9
    posteriori[np.isnan(posteriori)] = 0e-9

    if not os.path.exists(restart_path + '/restart'):
        os.makedirs(restart_path + '/restart')

    nc_file_path = os.path.join(
        restart_path, 'restart',
        'LE_%s_state_%s.nc' % (run_id, time.strftime('%Y%m%d_%H%M')))

    with nc.Dataset(nc_file_path, 'r+') as nc_obj:
        nc_obj.variables['c'][:] = posteriori


### eliminate all the values that less then 0 ###
def kill_negative(data: np.ndarray, fill_value=1e-9) -> np.ndarray:

    data[data <= 0] = fill_value
    data[np.isnan(data)] = fill_value

    return data


def convert_3d(posteriori_2d: np.ndarray, nlevel: int, Nspec: int, nlat: int,
               nlon: int, mass_partition: np.ndarray,
               spec_partition: np.ndarray) -> np.ndarray:

    posteriori_2d = posteriori_2d.reshape([nlat, nlon])
    posteriori_3d = np.zeros([Nspec, nlevel, nlat, nlon])

    for level in range(nlevel):
        for spec in range(Nspec):
            posteriori_3d[spec, level, :, :] = (posteriori_2d[:,:] * mass_partition[level, :, :]) \
                                                * spec_partition[level, spec]

    return posteriori_3d


### allocate the structure to the full space ###
def allocate2full(
    data_2d: np.ndarray,
    ratio: np.ndarray,
    Nlon: int,
    Nlat: int,
) -> np.ndarray:

    data_2d = data_2d.reshape([Nlat, Nlon])
    data_3d = np.zeros(ratio.shape)

    data_3d = data_2d * ratio
    data_3d = kill_negative(data_3d)

    return data_3d
