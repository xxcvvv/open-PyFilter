'''
Autor: Mijie Pang
Date: 2023-04-22 19:28:48
LastEditTime: 2023-09-23 09:20:11
Description: 
'''
import os
import numpy as np
import pandas as pd
from datetime import datetime


def find_nearest(point, line):
    line = np.asarray(line)
    if point > np.max(line) or point < np.min(line):
        return np.nan
    else:
        idx = (np.abs(line - point)).argmin()
        return idx


### seperate a time_range by the day ###
def seperate_time(time_range: list, type='day') -> list:

    ### get the time index ###
    if type == 'day':
        index = time_range.day - 1

    time_range_day = [[] for i in range(max(index) + 1)]
    for i_index in range(len(time_range)):
        time_range_day[index[i_index]].append(time_range[i_index])

    return time_range_day


def create_empty_table(path: str,
                       enkf_time_interval: str,
                       project_name: str,
                       model_first_run_time=None,
                       model_last_run_time='') -> None:

    if not os.path.exists(path + '/forecast/' + project_name):
        os.makedirs(path + '/forecast/' + project_name)

    csv_path = path + '/' + project_name + '_rmse.csv'

    if not os.path.exists(csv_path):

        empty_table = pd.DataFrame()

        model_window = pd.date_range(model_first_run_time,
                                     model_last_run_time,
                                     freq='1H')
        enkf_window = pd.date_range(model_first_run_time,
                                    model_last_run_time,
                                    freq=enkf_time_interval)
        model_window = [
            datetime.strftime(date, '%Y%m%d_%H%M') for date in model_window
        ]
        enkf_window = [
            datetime.strftime(date, '%Y%m%d_%H%M') for date in enkf_window[:-1]
        ]

        empty_table['Time'] = model_window
        for i in range(len(enkf_window)):
            empty_table[enkf_window[i]] = np.nan

        empty_table.to_csv(csv_path, index=None)


def save_rmse(rmse:float, time='', project_name='', path='')->None:

    csv_path = path + '/' + project_name + '_rmse.csv'

    rmse_file = pd.read_csv(csv_path)
    rmse_file = pd.DataFrame(rmse_file).set_index('Time')

    rmse_file.loc[time.strftime('%Y%m%d_%H%M')][time.strftime(
        '%Y%m%d_%H%M')] = rmse

    rmse_file.to_csv(csv_path)
