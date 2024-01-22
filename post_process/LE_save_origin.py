'''
Autor: Mijie Pang
Date: 2023-04-22 19:45:15
LastEditTime: 2023-08-21 14:42:11
Description: 
'''
import os
import sys
import subprocess
import pandas as pd
from glob import glob
from datetime import datetime

sys.path.append('../')
from system_lib import read_json

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Model = read_json(path=config_dir + '/Model.json')
Assimilation = read_json(path=config_dir + '/Assimilation.json')
Info = read_json(path=config_dir + '/Info.json')
Status = read_json(path=status_path)

model_scheme = Model['scheme']['name']
assi_scheme = Assimilation['scheme']['name']
iteration_num = Model[model_scheme]['iteration_num']

assimilation_time = datetime.strptime(
    Status['assimilation']['assimilation_time'], '%Y-%m-%d %H:%M:%S')
time_range = pd.date_range(Status['model']['start_time'],
                           Status['model']['end_time'],
                           freq=Model[model_scheme]['output_time_interval'])

target_dir = os.path.join(Info['path']['output_path'],
                          Model[model_scheme]['run_project'],
                          Assimilation[assi_scheme]['project_name'],
                          'forecast', time_range[0].strftime('%Y%m%d_%H%M'),
                          'forecast_files')

data_type = Model[model_scheme]['post_process']['data_type']
save_tool = Model[model_scheme]['post_process']['save_tool']

if Model[model_scheme]['run_type'] == 'ensemble':

    for i_ensem in range(int(Model[model_scheme]['ensemble_number'])):

        run_id = 'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)

        source_dir = os.path.join(
            Model[model_scheme]['path']['model_output_path'],
            Model[model_scheme]['run_project'], run_id)

        ### get the model output and restart file list ###
        if 'output' in data_type:
            source_dir_output = source_dir + '/output'
            target_dir_output = target_dir + '/' + run_id + '/output'
            if not os.path.exists(target_dir_output):
                os.makedirs(target_dir_output)
            output_file_list = []

        if 'restart' in data_type:
            source_dir_restart = source_dir + '/restart'
            target_dir_restart = target_dir + '/' + run_id + '/restart'
            if not os.path.exists(target_dir_restart):
                os.makedirs(target_dir_restart)

        ### save the results to the destinated directory ###
        for i_time in range(len(time_range)):

            ### save restart files ###
            if 'restart' in data_type:
                source_file = source_dir_restart + '/LE_%s_state_%s.nc' % (
                    run_id, time_range[i_time].strftime('%Y%m%d_%H%M'))

                if not i_time == 0:
                    subprocess.run(
                        [save_tool, source_file, target_dir_restart])

                else:
                    subprocess.run(
                        ['rsync', '-a', source_file, target_dir_restart])

            ### record output fils list and save them in batch ###
            if 'output' in data_type:
                output_file_list += glob(
                    source_dir_output + '/LE_%s_conc*_%s.nc' %
                    (run_id, time_range[i_time].strftime('%Y%m%d')))

        output_file_list = list(set(output_file_list))
        for i_file in range(len(output_file_list)):

            subprocess.run(
                [save_tool, output_file_list[i_file], target_dir_output])

elif Model[model_scheme]['run_type'] == 'ensemble_extend':

    time_set = Assimilation[assi_scheme]['time_set']
    ensemble_set = Assimilation[assi_scheme]['ensemble_set']
    ensemble_set = [
        int(ensemble_set[i_ensem]) for i_ensem in range(len(ensemble_set))
    ]
    run_ids = [[
        't_' + str(time_set[i_time]) + '_e_%02d' % (i_ensem)
        for i_ensem in range(ensemble_set[i_time])
    ] for i_time in range(len(time_set))]

    ################################################
    ###    save model output or restart files    ###
    for i_set in range(len(time_set)):
        for i_ensem in range(ensemble_set[i_set]):

            run_id = run_ids[i_set][i_ensem]
            source_dir = os.path.join(
                Model[model_scheme]['path']['model_output_path'],
                Model[model_scheme]['run_project'], run_id)

            if 'output' in data_type:
                source_dir_output = source_dir + '/output'
                target_dir_output = target_dir + '/' + run_id + '/output'
                if not os.path.exists(target_dir_output):
                    os.makedirs(target_dir_output)
                output_file_list = []

            if 'restart' in data_type:
                source_dir_restart = source_dir + '/restart'
                target_dir_restart = target_dir + '/' + run_id + '/restart'
                if not os.path.exists(target_dir_restart):
                    os.makedirs(target_dir_restart)

            for i_time in range(len(time_range)):

                if 'restart' in data_type:
                    source_file = source_dir_restart + '/LE_%s_state_%s.nc' % (
                        run_id, time_range[i_time].strftime('%Y%m%d_%H%M'))
                    if not i_time == 0:

                        subprocess.run(
                            [save_tool, source_file, target_dir_restart])
                    else:

                        subprocess.run(
                            ['rsync', '-a', source_file, target_dir_restart])

                if 'output' in data_type:
                    output_file_list += glob(
                        source_dir_output + '/LE_%s_*_%s.nc' %
                        (run_id, time_range[i_time].strftime('%Y%m%d')))

            output_file_list = list(set(output_file_list))
            for i_file in range(len(output_file_list)):

                subprocess.run(
                    [save_tool, output_file_list[i_file], target_dir_output])
