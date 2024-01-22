'''
Autor: Mijie Pang
Date: 2023-04-22 19:47:26
LastEditTime: 2023-12-21 09:22:09
Description: 
'''
import os
import subprocess
import pandas as pd
from glob import glob


def restore_nc(source_dir: str,
               target_dir: str,
               Ne: int,
               time: any,
               iteration_num=0) -> None:

    for i_ensem in range(Ne):

        run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)

        if not os.path.exists(target_dir + '/' + run_id + '/restart'):
            os.makedirs(target_dir + '/' + run_id + '/restart')

        source_file = os.path.join(
            source_dir, run_id, 'restart',
            'LE_%s_state_%s.nc' % (run_id, time.strftime('%Y%m%d_%H%M')))
        target_file = os.path.join(
            target_dir, run_id, 'restart,',
            'LE_%s_state_%s.nc' % (run_id, time.strftime('%Y%m%d_%H%M')))

        # command = os.system('rsync -a ' + source_file + ' ' + target_file)
        subprocess.run(['rsync', '-a', source_file, target_file])


def backup_files(backup_path: str, model_output_path: str,
                 run_id: str) -> None:

    output_dir = os.path.join(backup_path, run_id, 'output')
    restart_dir = os.path.join(backup_path, run_id, 'restart')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(restart_dir):
        os.makedirs(restart_dir)

    command = os.system('rsync -a %s/%s/output/* %s' %
                        (model_output_path, run_id, output_dir))
    command = os.system('rsync -a %s/%s/restart/* %s' %
                        (model_output_path, run_id, restart_dir))


def save_files(target_dir: str,
               source_dir: str,
               run_ids: list,
               start_time='',
               end_time='',
               freq='1H',
               save_output=True,
               save_restart=True,
               tool='rsync -a'):

    time_range = pd.date_range(start_time, end_time, freq=freq)
    tool = tool.split(' ')

    if save_restart:

        for i_id in range(len(run_ids)):

            if not os.path.exists(target_dir + '/' + run_ids[i_id] +
                                  '/restart'):
                os.makedirs(target_dir + '/' + run_ids[i_id] + '/restart')

            for i_time in range(len(time_range)):

                nc_file_restart_source = os.path.join(
                    source_dir, run_ids[i_id], 'restart', 'LE_%s_state_%s.nc' %
                    (run_ids[i_id],
                     time_range[i_time].strftime('%Y%m%d_%H%M')))

                nc_file_restart_target = os.path.join(target_dir,
                                                      run_ids[i_id], 'restart')

                # command = os.system(tool + ' ' + nc_file_restart_source + ' ' +
                #                     nc_file_restart_target)
                subprocess.run(
                    tool + [nc_file_restart_source, nc_file_restart_target])

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
                # command = os.system(tool + ' ' + output_file_list[i_file] +
                #                     ' ' + target_dir_output)
                subprocess.run(tool +
                               [output_file_list[i_file], target_dir_output])
