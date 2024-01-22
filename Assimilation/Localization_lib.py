'''
Autor: Mijie Pang
Date: 2023-09-23 16:44:44
LastEditTime: 2024-01-08 20:52:13
Description: 
'''
import logging
import numpy as np


class Local_class:

    def __init__(self,
                 x1_lon: np.ndarray,
                 x1_lat: np.ndarray,
                 x2_lon: np.ndarray,
                 x2_lat: np.ndarray,
                 distance_threshold: int,
                 distance_cal='empirical',
                 **kwargs) -> None:

        methods = {
            'empirical': self.empirical_distance,
            'haversine': self.haversine_distance
        }

        if distance_cal not in methods.keys():
            raise ValueError('Invalid distance calculation method -> "%s" <-' %
                             (distance_cal))

        Distance = methods.get(distance_cal)(x1_lon, x1_lat, x2_lon, x2_lat,
                                             **kwargs)

        self.Distance = Distance
        self.distance_threshold = distance_threshold

    ### empirical distance calculation ###
    def empirical_distance(self, x1_lon: np.ndarray, x1_lat: np.ndarray,
                           x2_lon: np.ndarray, x2_lat: np.ndarray,
                           **kwargs) -> np.ndarray:

        if kwargs.get('meshgrid1', False):
            x1_lon, x1_lat = np.meshgrid(x1_lon, x1_lat)
            x1_lon = x1_lon.ravel()
            x1_lat = x1_lat.ravel()

        if kwargs.get('meshgrid2', False):
            x2_lon, x2_lat = np.meshgrid(x2_lon, x2_lat)
            x2_lon = x2_lon.ravel()
            x2_lat = x2_lat.ravel()

        lon_delta = np.subtract.outer(x1_lon, x2_lon)  # dims : dim1 * dim2
        lat_delta = np.subtract.outer(x1_lat, x2_lat)  # dims : dim1 * dim2

        Distance = np.sqrt((lon_delta * 100)**2 + (lat_delta * 100)**2)
        return Distance

    ### Haversine distance calculation from "https://blog.csdn.net/XB_please/article/details/108213196"
    def haversine_distance(self, x1_lon: np.ndarray, x1_lat: np.ndarray,
                           x2_lon: np.ndarray, x2_lat: np.ndarray,
                           **kwargs) -> np.ndarray:

        if kwargs.get('meshgrid1', False):
            x1_lon, x1_lat = np.meshgrid(x1_lon, x1_lat)
            x1_lon = x1_lon.ravel()
            x1_lat = x1_lat.ravel()

        if kwargs.get('meshgrid2', False):
            x2_lon, x2_lat = np.meshgrid(x2_lon, x2_lat)
            x2_lon = x2_lon.ravel()
            x2_lat = x2_lat.ravel()

        x1_lon = np.radians(x1_lon.reshape([len(x1_lon), 1]))
        x1_lat = np.radians(x1_lat.reshape([len(x1_lat), 1]))
        x2_lon = np.radians(x2_lon)
        x2_lat = np.radians(x2_lat)

        lon_delta = x2_lon - x1_lon
        lat_delta = x2_lat - x1_lat

        a = np.sin(
            lat_delta /
            2)**2 + np.cos(x1_lat) * np.cos(x2_lat) * np.sin(lon_delta / 2)**2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
        Distance = 6371 * c

        return Distance

    ### calculate the Localization matrix ###
    def calculate(self, option='default') -> np.ndarray:

        options = {
            'default': self.default,
            '1': self.option1,
            '2': self.option2
        }
        return options.get(option)()

    def default(self, ) -> np.ndarray:

        Local_martrix = self.Distance / self.distance_threshold
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

    def option1(self) -> np.ndarray:
        pass

    def option2(self) -> np.ndarray:
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

    from datetime import datetime

    timer0 = datetime.now()

    model_lon = np.linspace(10, 20, 10)
    model_lat = np.linspace(10, 20, 10)
    local = Local_class(np.linspace(70, 140, 280),
                        np.linspace(15, 50, 140),
                        np.linspace(70, 140, 40),
                        np.linspace(70, 140, 40),
                        distance_threshold=500,
                        meshgrid1=True,
                        meshgrid2=True)
    Distance1 = local.Distance
    L1 = local.calculate()

    timer1 = datetime.now()
    # print(f'shape : {L1.shape}')
    print(f'shape : {Distance1.shape}')
    print('took %s s' % ((timer1 - timer0).total_seconds()))

    # L2 = localization(model_lon, model_lat, 500)
    # timer2 = datetime.now()

    # print(f'shape : {L2.shape}')
    # print('took %s s' % ((timer2 - timer1).total_seconds()))

    timer3 = datetime.now()
    local = Local_class(np.linspace(70, 140, 280),
                        np.linspace(15, 50, 140),
                        np.linspace(70, 140, 40),
                        np.linspace(70, 140, 40),
                        distance_threshold=500,
                        distance_cal='haversine',
                        meshgrid1=True,
                        meshgrid2=True)
    Distance2 = local.Distance
    L3 = local.calculate()

    timer4 = datetime.now()
    print(f'shape : {Distance2.shape}')
    print('took %s s' % ((timer4 - timer3).total_seconds()))
