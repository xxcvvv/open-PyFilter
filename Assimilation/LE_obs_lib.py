'''
Autor: Mijie Pang
Date: 2023-10-23 20:24:08
LastEditTime: 2024-01-08 21:25:19
Description: 
'''
import os
import logging
import numpy as np
import pandas as pd
from typing import List
from datetime import datetime


### get observation data ###
def read_obs(obs_dir: str,
             time: None,
             model_lon: np.ndarray,
             model_lat: np.ndarray,
             screen=False) -> None:

    obs_path = '%s/BC_PM10_%sUTC.csv' % (obs_dir,
                                         time.strftime('%Y_%m_%d_%H%M'))

    data = pd.read_csv(obs_path, header=None)

    if screen:

        data.dropna(axis=0, how='any')

        data = data.drop(data[(data.iloc[:, 0] < np.min(model_lon))
                              | (data.iloc[:, 0] > np.max(model_lon))].index)
        data = data.drop(data[(data.iloc[:, 1] < np.min(model_lat))
                              | (data.iloc[:, 1] > np.max(model_lat))].index)

    return data


def assign_error(obs_value: float, threshold=200, factor=0.1) -> float:

    if obs_value < threshold:
        return threshold
    else:
        return (obs_value - threshold) * factor + threshold


##########################################################################
### some useful functions
### check if the data has beyong the boundary ###
def check_bound(data: pd.DataFrame, bound1: List[float],
                bound2: List[float]) -> pd.DataFrame:

    data = data.drop(data[(data.iloc[:, 0] < bound1[0])
                          | (data.iloc[:, 0] > bound1[1])].index)
    data = data.drop(data[(data.iloc[:, 1] < bound2[0])
                          | (data.iloc[:, 1] > bound2[1])].index)

    return data


### find the index of the nearest point ###
def find_nearest(point: float, line: np.ndarray) -> int:

    idx = np.argmin(np.abs(np.asarray(line) - point))

    return idx


##########################################################################
### below are specific methods
### *--- Data Methods ---* ###
### data from air quality stations ###
def bc_pm10(time: None, obs_dir: str, dir_name: str, *args,
            **kwargs) -> pd.DataFrame or None:

    obs_path = '%s/%s/%s/BC_PM10_%sUTC.csv' % (
        obs_dir, dir_name, time.strftime('%Y'), time.strftime('%Y%m%d_%H%M'))

    if not os.path.exists(obs_path):
        FileNotFoundError('No such file -> "%s"' % (obs_path))
        return None

    df = pd.read_csv(obs_path)

    if kwargs.get('check_bound', False):
        df = check_bound(df, kwargs['bound1'], kwargs['bound2'])
    if kwargs.get('dropnan', True):
        df = df.dropna(axis=0, how='any')

    return df


### data from MODIS ###
def modis_dod(time: datetime, obs_dir: str, dir_name: str, *args,
              **kwargs) -> pd.DataFrame:

    obs_path = '%s/%s/%s/MODIS_DOD_%sUTC.csv' % (
        obs_dir, dir_name, time.strftime('%Y'), time.strftime('%Y%m%d_%H%M'))

    if not os.path.exists(obs_path):
        FileNotFoundError('No such file -> "%s"' % (obs_path))
        return None

    df = pd.read_csv(obs_path)
    df = df[['Longitude', 'Latitude', 'DOD_550_DB_DT']]
    df = df[df['DOD_550_DB_DT'] > 1e-3]

    if kwargs.get('check_bound', False):
        df = check_bound(df, kwargs['bound1'], kwargs['bound2'])
    if kwargs.get('dropnan', True):
        df = df.dropna(axis=0, how='any')

    return df


### data from VIIRS ###
def viirs_dod(time: None, obs_dir: str, dir_name: str, *args,
              **kwargs) -> pd.DataFrame:

    dir_name = kwargs.get('dir_name', dir_name)
    obs_path = '%s/%s/%s/VIIRS_DOD_%sUTC.csv' % (
        obs_dir, dir_name, time.strftime('%Y'), time.strftime('%Y%m%d_%H%M'))

    if not os.path.exists(obs_path):
        FileNotFoundError('No such file -> "%s"' % (obs_path))
        return None

    df = pd.read_csv(obs_path)
    df = df[['Longitude', 'Latitude', 'DOD_550_Land_Ocean']]
    df = df[df['DOD_550_Land_Ocean'] > 1e-3]

    if kwargs.get('check_bound', False):
        df = check_bound(df, kwargs['bound1'], kwargs['bound2'])
    if kwargs.get('dropnan', True):
        df = df.dropna(axis=0, how='any')

    return df


### *--- Error Methods ---* ###
### assign the fractional erros ###
def fraction_error(value: np.ndarray, *args, **kwargs) -> np.ndarray:

    threshold = kwargs.get('threshold', 200)
    factor = kwargs.get('factor', 0.1)

    condition = value > threshold

    error = np.zeros(len(value))
    error = np.where(condition, (value - threshold) * factor + threshold,
                     threshold)

    return error


### *--- Map Methods ---* ###
### the nearest search method ###
def nearest_search(data: pd.DataFrame, *args, **kwargs) -> np.ndarray:

    model_lon = kwargs['model_lon']
    model_lat = kwargs['model_lat']

    map_lon_idx = np.array([
        find_nearest(data.iloc[i_obs, 0], model_lon)
        for i_obs in range(len(data))
    ])
    map_lat_idx = np.array([
        find_nearest(data.iloc[i_obs, 1], model_lat)
        for i_obs in range(len(data))
    ])
    map_idx = map_lat_idx * len(model_lon) + map_lon_idx

    return map_idx


### another version of nearest search for multi-layers ###
### TO BE FINISHED !!!!!!!
# def nearest_search_layers(self, data: pd.DataFrame, *args,
#                           **kwargs) -> np.ndarray:

#     model_lon = kwargs['model_lon']
#     model_lat = kwargs['model_lat']

#     map_lon_idx = np.array([
#         find_nearest(data.iloc[i_obs, 0], model_lon)
#         for i_obs in range(len(data))
#     ])
#     map_lat_idx = np.array([
#         find_nearest(data.iloc[i_obs, 1], model_lat)
#         for i_obs in range(len(data))
#     ])
#     map_idx = map_lat_idx * len(model_lon) + map_lon_idx

#     return map_idx


### merge all the surrounding points to grid ###
def merge_surrounding():
    pass


### *--- H Operators ---* ###
def H_linear(H: np.ndarray, *args, **kwargs) -> np.ndarray:

    if 'factor' in kwargs.keys():
        conversion_factor = kwargs['factor']
    else:
        conversion_factor = args[0] / args[1]

    conversion_factor[np.isnan(conversion_factor)] = 0
    H = H * conversion_factor.reshape(1, -1)

    return H


### *--- for one observation only !!! ---* ###
class Observation:

    def __init__(self, obs_dir: str, obs_type: str) -> None:

        data_methods = {
            'bc_pm10': bc_pm10,
            'modis_dod': modis_dod,
            'viirs_dod': viirs_dod
        }
        if not obs_type in data_methods.keys():
            raise ValueError('Observation -> "%s" <- is not regonized.' %
                             (obs_type))

        self.data_methods = data_methods
        self.obs_dir = obs_dir
        self.obs_type = obs_type

    ##########################################################################
    ### method portals for observation operations
    ### these portals must be used *in turn* !!!
    ### *--- Method Portals ---* ###

    ### get the observation data ###
    def get_data(self, time: datetime, dir_name: str, *args, **kwargs) -> None:

        data = self.data_methods.get(self.obs_type)(time, self.obs_dir,
                                                    dir_name, *args, **kwargs)

        if data is None:
            m = 0
        else:
            m = len(data)  # m is the number of observations
            values = np.array(data.iloc[:, 2]).reshape([m, 1])

        self.m = m
        self.data = data
        self.values = values

    ### map the model space into observation space ###
    def map2obs(self, method: str, model_lon: np.ndarray,
                model_lat: np.ndarray, *args, **kwargs) -> None:

        methods = {"nearest": nearest_search}

        if method not in methods.keys():
            raise ValueError('Method -> "%s" <- is not regonized.' % (method))

        map_index = methods.get(method)(self.data, model_lon, model_lat, *args,
                                        **kwargs)

        H = np.zeros([self.m, int(len(model_lon) * len(model_lat))])
        H[np.arange(self.m), map_index] = 1

        self.H = H

    ### get the observational error ###
    def get_error(self, method: str, *args, **kwargs) -> None:

        error_methods = {"fraction": fraction_error}

        if not method in error_methods.keys():
            raise ValueError('Method -> "%s" <- is not regonized.' % (method))

        error = error_methods.get(method)(self.values, *args, **kwargs)
        O = np.diag(error**2)

        self.error = error
        self.O = O

    ### operate on the H, this is not necessary part ###
    def H_operator(self, method: str, *args, **kwargs) -> None:

        methods = {'linear', H_linear}

        if not method in methods.keys():
            raise ValueError('Method -> "%s" <- is not regonized.' % (method))

        H = methods.get(method)(self.H, *args, **kwargs)

        self.H = H


### *--- for multiple observations ---* ###
class Observations:

    def __init__(self, obs_dir: str) -> None:

        self.obs_dir = obs_dir
        self.Observation = Observation

        self.data = {}
        self.values = {}
        self.map_idx = {}
        self.error = {}
        self.m = {}

    ##########################################################################
    ### method portals for observation operations
    ### these portals must be used *in turn* !!!
    ### *--- Method Portals ---* ###

    ### *--- get the observation data ---* ###
    ### this function should be run as many as the observation types
    ### before doing following procedures
    def get_data(self, obs_type: str, time: datetime, dir_name: str, *args,
                 **kwargs) -> None:

        data_methods = {
            'bc_pm10': bc_pm10,
            'modis_dod': modis_dod,
            'viirs_dod': viirs_dod
        }
        if not obs_type in data_methods.keys():
            raise ValueError('Observation -> "%s" <- is not regonized.' %
                             (obs_type))

        data = data_methods.get(obs_type)(time, self.obs_dir, dir_name, *args,
                                          **kwargs)

        if data is None:
            m = 0
            values = np.empty(0)
            logging.warning('No observations received from %s' % (obs_type))
        else:
            # m is the number of observations
            m = len(data)
            values = np.array(data.iloc[:, 2]).reshape([m, 1])
            logging.info('%s observations received from %s' % (m, obs_type))

        self.m[obs_type] = m
        self.data[obs_type] = data
        self.values[obs_type] = values
        self.obs_type = obs_type

    ### *--- map the model space into observation space ---* ###
    def map2obs(self, method: str, *args, **kwargs) -> None:

        repeat = kwargs.get('repeat', 1)

        if not self.m[self.obs_type] == 0:

            methods = {"nearest": nearest_search}

            if method not in methods.keys():
                raise ValueError('Method -> "%s" <- is not regonized.' %
                                 (method))

            map_idx = methods.get(method)(self.data[self.obs_type], *args,
                                          **kwargs)

            if repeat > 1:
                map_idx = np.tile(map_idx, (repeat, 1))

            self.map_idx[self.obs_type] = map_idx

        else:

            self.map_idx[self.obs_type] = np.empty(0, dtype=int)

    ### *--- get the observational error ---* ###
    def get_error(self, method: str, *args, **kwargs) -> None:

        # has observations
        if not self.m[self.obs_type] == 0:

            error_methods = {"fraction": fraction_error}

            if not method in error_methods.keys():
                raise ValueError('Method -> "%s" <- is not regonized.' %
                                 (method))

            error = error_methods.get(method)(self.values[self.obs_type],
                                              *args, **kwargs)

            self.error[self.obs_type] = error

        # no observations
        else:

            self.error[self.obs_type] = np.empty(0)

    # layering the original data, designed for AOD-like observations
    def layering(self, layers: np.ndarray, **kwargs) -> None:

        if not self.m[self.obs_type] == 0:

            values = np.zeros(self.map_idx[self.obs_type].shape)
            error = np.zeros(self.map_idx[self.obs_type].shape)
            for i_lev in range(len(layers)):
                layer = layers[i_lev, self.map_idx[self.obs_type][i_lev, :]]
                values[i_lev, :] = self.values[self.obs_type].flatten() * layer
                error[i_lev, :] = self.error[self.obs_type].flatten() * layer

            self.values[self.obs_type] = values
            self.error[self.obs_type] = error

    ### filter the observations out of the model sapce ###
    def local_filter(self, local_bools: np.ndarray) -> None:

        if local_bools.ndim == 1:
            if not self.m[self.obs_type] == 0:

                map_bools = local_bools[self.map_idx[self.obs_type]]

                self.map_idx[self.obs_type] = self.map_idx[
                    self.obs_type][map_bools]
                self.values[self.obs_type] = self.values[
                    self.obs_type][map_bools]
                self.error[self.obs_type] = self.error[
                    self.obs_type][map_bools]

        elif local_bools.ndim == 2:
            if not self.m[self.obs_type] == 0:

                map_bools = np.take(local_bools,
                                    self.map_idx[self.obs_type][0],
                                    axis=1)

                self.map_idx[self.obs_type] = [
                    np.array(self.map_idx[self.obs_type][i_lev, :][map_bools[
                        i_lev, :]]) for i_lev in range(len(map_bools))
                ]
                self.values[self.obs_type] = [
                    np.array(self.values[self.obs_type][i_lev, :][map_bools[
                        i_lev, :]]) for i_lev in range(len(map_bools))
                ]
                self.error[self.obs_type] = [
                    np.array(self.error[self.obs_type][i_lev, :][map_bools[
                        i_lev, :]]) for i_lev in range(len(map_bools))
                ]

    ### *--- reduce the dimension of the variables ---* ###
    def reduce_dim(self, Ns: int, Nlev: int) -> None:

        if not self.m[self.obs_type] == 0:

            map_idx = []
            for i_lev in range(Nlev):
                map_idx.append(self.map_idx[self.obs_type][i_lev] + i_lev * Ns)

            self.map_idx[self.obs_type] = np.concatenate(
                self.map_idx[self.obs_type])
            self.values[self.obs_type] = np.concatenate(
                self.values[self.obs_type])
            self.error[self.obs_type] = np.concatenate(
                self.error[self.obs_type])

    ### *-----------------------------------* ###
    ### *---   assemble some variables   ---* ###
    def gather_map_idx(self, *args) -> np.ndarray:

        non_empty_values = [
            self.map_idx[key] for key in args if len(self.map_idx[key]) > 0
        ]
        map_idx_gathered = np.concatenate(non_empty_values)

        return map_idx_gathered


### *--- observation class for ground ---* ###
class ObservationTypeOne:

    def __init__(self) -> None:
        pass


### *--- observation class for AOD ---* ###
class ObservationTypeTwo:

    def __init__(self) -> None:
        pass


class ObsrvationTypeThree:

    def __init__(self) -> None:
        pass


if __name__ == '__main__':

    obs = Observation(
        '/home/pangmj/Data/pyFilter/observation/Asml_MODIS_DOD_UTC', 'modis')
    obs.get_data(datetime.strptime('2021-03-15 07:00', '%Y-%m-%d %H:%M'))
    obs.map2obs('nearest', np.arange(70, 140, 0.25), np.arange(15, 50, 0.25))
    obs.get_error('fraction', threshold=0.1, factor=0.1)
    print(obs.data, type(obs.data))
    print(obs.values, type(obs.values))
    print(obs.error, type(obs.error))
    print(obs.H, type(obs.H))
    print(obs.O, type(obs.O))
    print(obs.m, type(obs.m))
