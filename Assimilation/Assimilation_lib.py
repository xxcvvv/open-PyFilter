'''
Autor: Mijie Pang
Date: 2023-04-22 19:15:58
LastEditTime: 2024-04-03 21:40:48
Description: this script is the library for the assimilation algorithms
'''
import os
import numpy as np
from numpy.linalg import inv
import netCDF4 as nc


def calculate_covariance_matrix(
        X_f: np.ndarray) -> np.ndarray:  # shape of X_f : Ne * Ns

    x_f_mean = np.mean(X_f, axis=0)
    X_f_pertubate = X_f - x_f_mean
    P_f = np.cov(X_f_pertubate.T, ddof=1)

    return P_f


def calculate_kalman_gain(P_f: np.ndarray, H: np.ndarray,
                          R: np.ndarray) -> np.ndarray:
    return P_f @ H.T @ inv(H @ P_f @ H.T + R)


def calculate_posteriori(x_f: np.ndarray, K: np.ndarray, y: np.ndarray,
                         H: np.ndarray) -> np.ndarray:
    return np.squeeze(x_f + K @ (y - H @ x_f))


def rsync_file(source_path: str, target_path: str) -> None:

    if isinstance(target_path, list):

        for i in range(len(target_path)):

            command = os.system('rsync -a ' + source_path + ' ' +
                                target_path[i])
            # subprocess.run(['rsync', '-a', source_path, target_path[i]])

    else:

        command = os.system('rsync -a ' + source_path + ' ' + target_path)
        # subprocess.run(['rsync', '-a', source_path, target_path])


def save2npy(dir_name: str, variables: dict) -> None:

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    # command = os.system('rm ' + dir_name + '/*')

    for key in variables:
        np.save(os.path.join(dir_name, key + '.npy'), variables[key])


def save2nc(dir_name: str, variable: dict, dim: dict) -> None:

    variable_name = list(variable.keys())[0]
    with nc.Dataset(dir_name + '/' + variable_name + '.nc',
                    'w',
                    format='NETCDF4') as new_nc:

        ### create dimensions ###
        for key in dim.keys():
            if isinstance(dim[key], int):
                new_nc.createDimension(key, size=dim[key])
            else:
                new_nc.createDimension(key, size=len(dim[key]))

        ### create variable for data ###
        create_variable = new_nc.createVariable(variable_name, 'f4', \
                                                dimensions=tuple(dim.keys()))
        create_variable[:] = variable[variable_name][:]

        ### create variable for dimensions ###
        create_variable = {}
        for key in dim.keys():
            if not isinstance(dim[key], int):
                create_variable[key] = new_nc.createVariable(key,
                                                             'f4',
                                                             dimensions=key)
                create_variable[key][:] = dim[key]
