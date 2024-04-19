'''
Autor: Mijie Pang
Date: 2024-04-03 17:30:06
LastEditTime: 2024-04-04 16:10:08
Description: 
'''
import time
import numpy as np
from numba import jit
from decorators import deprecated


@jit(nopython=True)
@deprecated
def find_nearest(point: float, line: np.ndarray) -> int:

    if point > line.max() or point < line.min():
        return np.nan
    else:
        idx = np.argmin(np.abs(line - point))
        return idx


def find_nearest_vector(points: np.ndarray,
                        line: np.ndarray,
                        thres=None) -> np.ndarray:

    idxs = np.searchsorted(line, points, side='left')
    idxs = np.clip(idxs, 1, len(line) - 1)
    left = line[idxs - 1]
    right = line[idxs]
    idxs -= points - left < right - points

    if not thres is None:
        distance = points - line[idxs]
        idxs[np.abs(distance) > thres] = -1

    return idxs


if __name__ == '__main__':

    a = np.linspace(0, 100, 100000)
    b = np.linspace(10, 100, 100)

    start = time.time()
    output = np.zeros(a.shape)
    for i in range(len(a)):
        output[i] = find_nearest(a[i], b)
    print('numba way took %.4f s' % (time.time() - start))

    start = time.time()
    output = find_nearest_vector(a, b)
    print('numpy way took %.4f s' % (time.time() - start))
