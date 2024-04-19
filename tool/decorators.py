'''
Autor: Mijie Pang
Date: 2024-04-17 11:24:46
LastEditTime: 2024-04-17 16:07:19
Description: 
'''
import time
import pickle
import warnings
import numpy as np


# measure time
def timer(func):

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(
            f'{func.__name__} took {end_time - start_time:.2f} seconds to execute.'
        )
        return result

    return wrapper


# indicate deprecated function
def deprecated(func):

    def wrapper(*args, **kwargs):
        warnings.warn(f'Call to deprecated function : {func.__name__}',
                      category=DeprecationWarning)
        return func(*args, **kwargs)

    return wrapper


# suppress errors and return None
def suppress_errors(func):

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f'Error in {func.__name__}: {e}')
            return None

    return wrapper


# memoize the function results
def memoize(func):
    cache = {}

    def wrapper(*args):
        if args in cache:
            return cache[args]
        result = func(*args)
        cache[args] = result
        return result

    return wrapper


# save function output to file
def cache(path='./test.pkl'):

    def decorator(func):

        def wrapper(*args, **kwargs):

            result = func(*args, **kwargs)

            if path.endswith('.pkl'):
                with open(path, 'wb') as file:
                    pickle.dump(result, file)

            elif path.endswith('.npy'):
                np.save(path, result)

            else:
                ValueError(
                    'Invalid file extension. Only .pkl and .npy are supported.'
                )

            return result

        return wrapper

    return decorator


if __name__ == '__main__':
    pass
