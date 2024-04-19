'''
Autor: Mijie Pang
Date: 2023-04-22 19:47:26
LastEditTime: 2024-03-05 14:28:13
Description: 
'''
import os
import subprocess
import pandas as pd
from glob import glob


def save_files(target_dir: str,
               source_dir: str,
               run_ids: list,
               start_time: str,
               end_time: str,
               freq='1H',
               save_output=True,
               save_restart=True,
               tool='rsync -a') -> None:

    time_range = pd.date_range(start_time, end_time, freq=freq)
    tool = tool.split(' ')

    if save_restart:

        for i_id in range(len(run_ids)):

            restart_target_dir = os.path.join(target_dir, run_ids[i_id],
                                              'restart')
            if not os.path.exists(restart_target_dir):
                os.makedirs(restart_target_dir)

            for i_time in range(len(time_range)):

                nc_file_restart_source = os.path.join(
                    source_dir, run_ids[i_id], 'restart', 'LE_%s_state_%s.nc' %
                    (run_ids[i_id],
                     time_range[i_time].strftime('%Y%m%d_%H%M')))

                subprocess.run(tool +
                               [nc_file_restart_source, restart_target_dir])

    if save_output:

        for i_id in range(len(run_ids)):

            source_dir_output = source_dir + '/' + run_ids[i_id] + '/output'
            target_dir_output = target_dir + '/' + run_ids[i_id] + '/output'

            if not os.path.exists(target_dir_output):
                os.makedirs(target_dir_output)

            output_file_list = []
            for i_time in range(len(time_range)):
                output_file_list += glob(
                    source_dir_output + '/LE_%s_*_%s.nc' %
                    (run_ids[i_id], time_range[i_time].strftime('%Y%m%d')))

            output_file_list = list(set(output_file_list))
            for i_file in range(len(output_file_list)):
                subprocess.run(tool +
                               [output_file_list[i_file], target_dir_output])
