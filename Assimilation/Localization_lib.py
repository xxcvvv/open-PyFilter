'''
Autor: Mijie Pang
Date: 2023-09-23 16:44:44
LastEditTime: 2024-04-05 20:02:58
Description: 
'''
import logging
import numpy as np
# from memory_profiler import profile


class Localization:

    def __init__(self, lon1: np.ndarray, lat1: np.ndarray, lon2: np.ndarray,
                 lat2: np.ndarray, **kwargs) -> None:

        self.lon1, self.lat1, self.lon2, self.lat2 = map(
            np.array, [lon1, lat1, lon2, lat2])

    def cal_distance(self, method='empirical', **kwargs) -> np.ndarray:

        methods = {
            'empirical': self.empirical_distance,
            'haversine': self.haversine_distance
        }
        if method not in methods.keys():
            raise ValueError('Invalid distance calculation method -> "%s" <-' %
                             (method))

        Distance = methods.get(method)(self.lon1, self.lat1, self.lon2,
                                       self.lat2, **kwargs)

        self.Distance = Distance

        return Distance

    ### *--- calculate the Localization matrix ---* ###
    def cal_correlation(self, option='default', **kwargs) -> np.ndarray:

        options = {
            'default': self.default,
            'linear': self.linear,
            'option': self.option
        }
        Correlation = options.get(option)(**kwargs)

        return Correlation

    def prepare_meshgrid(self, lon: np.ndarray, lat: np.ndarray,
                         meshgrid_needed: bool) -> np.ndarray:
        if meshgrid_needed:
            lon, lat = np.meshgrid(lon, lat)
            lon, lat = lon.ravel(), lat.ravel()
        return lon, lat

    ### *--------------------------------------* ###
    ### *---      Distance Calculation      ---* ###

    ### empirical distance calculation ###
    # @profile
    def empirical_distance(self, lon1: np.ndarray, lat1: np.ndarray,
                           lon2: np.ndarray, lat2: np.ndarray,
                           **kwargs) -> np.ndarray:

        lon1, lat1 = self.prepare_meshgrid(lon1, lat1,
                                           kwargs.get('meshgrid1', False))
        lon2, lat2 = self.prepare_meshgrid(lon2, lat2,
                                           kwargs.get('meshgrid2', False))

        lon_delta = np.subtract.outer(lon1, lon2)  # dims : dim1 * dim2
        lat_delta = np.subtract.outer(lat1, lat2)  # dims : dim1 * dim2

        Distance = np.sqrt((lon_delta * 100)**2 + (lat_delta * 100)**2)

        return Distance

    ### Haversine distance calculation from "https://blog.csdn.net/XB_please/article/details/108213196"
    # @profile
    def haversine_distance(self, lon1: np.ndarray, lat1: np.ndarray,
                           lon2: np.ndarray, lat2: np.ndarray,
                           **kwargs) -> np.ndarray:

        lon1, lat1 = self.prepare_meshgrid(lon1, lat1,
                                           kwargs.get('meshgrid1', False))
        lon2, lat2 = self.prepare_meshgrid(lon2, lat2,
                                           kwargs.get('meshgrid2', False))

        lon1, lat1, lon2, lat2 = map(np.radians, [
            lon1.reshape([len(lon1), 1]),
            lat1.reshape([len(lat1), 1]), lon2, lat2
        ])

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = np.sin(
            dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2

        # mean radius of earth (km)
        Distance = 6371.0 * 2 * np.arcsin(np.sqrt(a))

        return Distance

    ### *------------------------------------* ###
    ### *---   Correlation Calculatioon   ---* ###
    # @profile
    def default(self, **kwargs) -> np.ndarray:

        distance_threshold = kwargs.get('distance_threshold', 500)
        Local_martrix = self.Distance / distance_threshold

        condition1 = Local_martrix < 1
        condition2 = np.logical_and(1 <= Local_martrix, Local_martrix < 2)
        condition3 = Local_martrix >= 2

        distance1 = Local_martrix[condition1]
        Local_martrix[condition1] = 1 - 5 / 3 * distance1**2 + 5 / 8 * distance1**3 + \
                                    1 / 2 * distance1**4 - 1 / 4 * distance1**5

        distance2 = Local_martrix[condition2]
        Local_martrix[condition2] = -2 / 3 * distance2**(-1) + 4 - 5 * distance2 + \
                                    5 / 3 * distance2**2 + 5 / 8 * distance2**3 - \
                                    1 / 2 * distance2**4 + 1 / 12 * distance2**5

        Local_martrix[condition3] = 0

        return Local_martrix

    def linear(self) -> np.ndarray:
        pass

    def option(self) -> np.ndarray:
        pass


def select_elements(array: np.ndarray, value: float, range: int) -> np.ndarray:

    bool_array = array > value
    mask = np.zeros_like(array, dtype=bool)

    indices = np.argwhere(bool_array)
    logging.debug(indices.shape, indices)
    idx_min = np.maximum(indices - range, 0)
    idx_max = np.minimum(indices + range + 1, array.shape)

    mask[tuple(
        zip(*[
            np.arange(start, stop)
            for start, stop in zip(idx_min.T, idx_max.T)
        ]))] = True

    return mask


if __name__ == '__main__':

    from time import time

    start = time()
    L1 = Localization(np.linspace(70, 140, 280), np.linspace(15, 50, 140),
                      np.linspace(70, 140, 40), np.linspace(70, 140, 40))
    L1.cal_distance(meshgrid1=True, meshgrid2=True)
    L1.cal_correlation(distance_threshold=500)
    print(f'shape : {L1.Distance.shape}')
    print('took %.2f s' % (time() - start))

    start = time()
    L2 = Localization(np.linspace(70, 140, 280), np.linspace(15, 50, 140),
                      np.linspace(70, 140, 40), np.linspace(70, 140, 40))
    L2.cal_distance('haversine', meshgrid1=True, meshgrid2=True)
    L2.cal_correlation(distance_threshold=500)
    print(f'shape : {L2.Distance.shape}')
    print('took %.2f s' % (time() - start))

    L = Localization([110, 100], [35, 30], [110, 100], [30, 30])
    d1 = L.cal_distance('empirical')
    print(L.cal_correlation())
    d2 = L.cal_distance('haversine')
    print(L.cal_correlation())
    print(d1, d2)
