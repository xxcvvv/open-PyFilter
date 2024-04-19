'''
Autor: Mijie Pang
Date: 2023-10-23 20:24:08
LastEditTime: 2024-04-17 19:20:14
Description: 
'''
import os
import sys
import logging
import numpy as np
import pandas as pd
from typing import List
from datetime import datetime

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
from tool.mapper import find_nearest_vector


### *--- Some useful functions ---* ###
# check if the data has beyong the boundary
def check_bound(data: pd.DataFrame, bound1: List[float],
                bound2: List[float]) -> pd.DataFrame:

    data = data.drop(data[(data.iloc[:, 0] < bound1[0])
                          | (data.iloc[:, 0] > bound1[1])].index)
    data = data.drop(data[(data.iloc[:, 1] < bound2[0])
                          | (data.iloc[:, 1] > bound2[1])].index)

    return data


### *------------------------------* ###
### *---      Data Methods      ---* ###
### *------------------------------* ###


def bc_pm10(time: None, obs_dir: str, dir_name: str, **kwargs) -> pd.DataFrame:
    """
    Get bias corrected PM10 (dust) data from air quality stations.

    params:
        time: the time of the data.
        obs_dir: directory of the data.
        dir_name: sub directory of the data.
        
    return:
        dataframe: includes the longitude, latitude and bias corrected dust data.
    """

    obs_path = os.path.join(obs_dir, dir_name, time.strftime('%Y'),
                            f"BC_PM10_{time.strftime('%Y%m%d_%H%M')}UTC.csv")

    if not os.path.exists(obs_path):
        FileNotFoundError(f"No such file -> \"{obs_path}\"")
        return None

    try:
        df = pd.read_csv(obs_path)
    except Exception as e:
        ValueError(f"Error reading the CSV file: {e}")
        return None

    if kwargs.get('check_bound', False):
        df = check_bound(df, kwargs['bound1'], kwargs['bound2'])
    if kwargs.get('dropnan', True):
        df = df.dropna(axis=0, how='any')

    return df


def modis_dod(time: datetime, obs_dir: str, dir_name: str, *args,
              **kwargs) -> pd.DataFrame:
    """
    Get MODIS DOD data from the specified directory.
    
    params:
    - time: the time of the data.
    - obs_dir: directory of the data.
    - dir_name: sub directory of the data.
    - kwargs: additional keyword arguments.
        - check_bound: whether to check the boundaries.
        - dropnan: whether to drop NaN values.
        - bound1: the first boundary.
        - bound2: the second boundary.

    return:
        dataframe: includes the longitude, latitude and DOD data.   
 
    """

    obs_path = os.path.join(
        obs_dir, dir_name, time.strftime('%Y'),
        f"MODIS_DOD_{time.strftime('%Y%m%d_%H%M')}UTC.csv")

    if not os.path.exists(obs_path):
        FileNotFoundError(f"No such file -> \"{obs_path}\"")
        return None

    try:
        df = pd.read_csv(obs_path)
    except Exception as e:
        ValueError(f"Error reading CSV file: {e}")
        return None

    relevant_columns = ['Longitude', 'Latitude', 'DOD_550_DB_DT']
    df = df[relevant_columns]

    df = df[df['DOD_550_DB_DT'] > 1e-3]

    if kwargs.get('check_bound', False):
        df = check_bound(df, kwargs['bound1'], kwargs['bound2'])
    if kwargs.get('dropnan', True):
        df = df.dropna(axis=0, how='any')

    return df


def viirs_dod(time: None, obs_dir: str, dir_name: str, *args,
              **kwargs) -> pd.DataFrame:
    """
    Load and process VIIRS DOD data from specified directory.

    Parameters:
    - time: The time instance for which the data is required.
    - obs_dir: The base directory where the observation data is stored.
    - dir_name: The sub-directory name within obs_dir where the specific data is located.
    - **kwargs: Additional keyword arguments including:
        - dir_name: Overwrites the default dir_name if provided.
        - check_bound: Boolean to indicate if boundary checking is required.
        - bound1, bound2: The boundary values for checking.
        - dropnan: Boolean to indicate if rows with any NaN values should be dropped.

    Returns:
    - A pandas DataFrame containing the processed data.
    """

    dir_name = kwargs.get('dir_name', dir_name)
    obs_path = os.path.join(
        obs_dir, dir_name, time.strftime('%Y'),
        f'VIIRS_DOD_{time.strftime("%Y%m%d_%H%M")}UTC.csv')

    # Properly raise an exception if the file does not exist
    if not os.path.exists(obs_path):
        FileNotFoundError(f"No such file -> '{obs_path}'")
        return None

    # Load the csv file into a DataFrame
    df = pd.read_csv(obs_path)

    # Filter DataFrame to include only relevant columns and values above a threshold
    df = df[['Longitude', 'Latitude', 'DOD_550_Land_Ocean']]
    df = df[df['DOD_550_Land_Ocean'] > 1e-3]

    if kwargs.get('check_bound', False):
        df = check_bound(df, kwargs['bound1'], kwargs['bound2'])
    if kwargs.get('dropnan', True):
        df = df.dropna(axis=0, how='any')

    return df


def himawari_8_dod(time: str, obs_dir: str, dir_name: str, *args,
                   **kwargs) -> pd.DataFrame:
    """
    Load and process Himawari-8 DOD data from specified directory.
    
    params:
        time: The time instance for which the data is required.
        obs_dir: The base directory where the observation data is stored.
        dir_name: The sub-directory name within obs_dir where the specific data is located.
        **kwargs: Additional keyword arguments including:
            - dir_name: Overwrites the default dir_name if provided.
            - check_bound: Boolean to indicate if boundary checking is required.
            - bound1, bound2: The boundary values for checking.
    
    return:
        A pandas DataFrame containing the data.
    """

    dir_name = kwargs.get('dir_name', dir_name)
    obs_path = os.path.join(
        obs_dir, dir_name, time.strftime('%Y'),
        f'Himawari_DOD_{time.strftime("%Y%m%d_%H%M")}UTC.csv')

    if not os.path.exists(obs_path):
        FileNotFoundError(f"No such file -> '{obs_path}'")
        return None

    df = pd.read_csv(obs_path)

    # 筛选条件
    is_valid = df['AE'] <= 1
    df = df.loc[is_valid, ['lon', 'lat', 'DOD']]
    df = df[df['DOD'] > 1e-3]

    if kwargs.get('check_bound', False):
        df = check_bound(df, kwargs['bound1'], kwargs['bound2'])
    if kwargs.get('dropnan', True):
        df = df.dropna(axis=0, how='any')

    return df


### *-------------------------------* ###
### *---       Map Methods       ---* ###
### *-------------------------------* ###


### *--- the nearest search method ---* ###
def nearest_search(data: pd.DataFrame, *args, **kwargs) -> np.ndarray:
    """
    find the nearest match and return an array of mapping indices.
    
    parameters:
        data: including longitude and latitude information at the first two columns.
        model_lon: model longitude.
        model_lat: model latitude.
        
    return:
        a numpy array of mapping indices.
    """

    # get the model longitude and latitude
    model_lon = kwargs['model_lon']
    model_lat = kwargs['model_lat']

    # vectorize the calculation
    obs_lon = data.iloc[:, 0].values
    obs_lat = data.iloc[:, 1].values

    map_lon_idx = find_nearest_vector(obs_lon, model_lon)
    map_lat_idx = find_nearest_vector(obs_lat, model_lat)

    map_idx = map_lat_idx * len(model_lon) + map_lon_idx

    return map_idx


### merge all the surrounding points to grid ###
def merge_surrounding():
    raise NotImplementedError('merge surrounding data not implemented yet')


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

### *-------------------------------* ###
### *---      Error Methods      ---* ###
### *-------------------------------* ###


def fraction_error(value: np.ndarray, *args, **kwargs) -> np.ndarray:
    """
    Calculate the fractional error for a given data array.
    
    params:
        value (np.ndarray): input observation array。
        
    return:
        error array.
    """

    threshold = kwargs.get('threshold', 200)
    factor = kwargs.get('factor', 0.1)

    condition = value > threshold

    error = np.zeros(len(value))
    error = np.where(condition, (value - threshold) * factor + threshold,
                     threshold)

    return error


### *--- for one observation only !!! ---* ###
class Observation:

    def __init__(self, obs_dir: str, obs_type: str, config: dict) -> None:

        self.obs_dir = obs_dir
        self.obs_type = obs_type
        self.config = config

    ### *--- Method Portals ---* ###
    '''
    method portals for observation operations, 
    these portals must be used *in turn* !!!
    '''

    ### *--- get the observation data ---* ###
    def get_data(self, time: datetime, *args, **kwargs) -> None:

        data_methods = {
            'bc_pm10': bc_pm10,
            'modis_dod': modis_dod,
            'viirs_dod': viirs_dod,
            'himawari_8_dod': himawari_8_dod
        }
        if not self.obs_type in data_methods.keys():
            raise ValueError('Observation -> "%s" <- is not regonized.' %
                             (self.obs_type))

        dir_name = self.config.get('dir_name')
        data = data_methods.get(self.obs_type)(time, self.obs_dir, dir_name,
                                               *args, **kwargs)

        if data is None:
            m = 0
            values = np.empty(0)
        else:
            m = len(data)  # m is the number of observations
            values = np.array(data.iloc[:, 2]).reshape([m, 1])

        logging.info('%s observations received from %s' % (m, self.obs_type))

        self.m = m
        self.data = data
        self.values = values

    ### *--- map the model space into observation space ---* ###
    def map2obs(self, method: str, *args, **kwargs) -> None:

        methods = {'nearest': nearest_search}

        if not self.m == 0:
            if method not in methods.keys():
                raise ValueError('Method -> "%s" <- is not regonized.' %
                                 (method))

            map_idx = methods.get(method)(self.data, *args, **kwargs)
        else:
            map_idx = np.empty(0, dtype=int)

        self.map_idx = map_idx

    ### *--- get the observational error ---* ###
    def get_error(self, method: str, *args, **kwargs) -> None:

        error_methods = {'fraction': fraction_error}

        if not self.m == 0:
            if not method in error_methods.keys():
                raise ValueError('Method -> "%s" <- is not regonized.' %
                                 (method))
            error = error_methods.get(method)(self.values, *args, **kwargs)
        else:
            error = np.empty(0)

        self.error = error


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
