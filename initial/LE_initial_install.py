'''
Autor: Mijie Pang
Date: 2023-03-27 19:23:23
LastEditTime: 2023-12-12 20:17:49
Description: designed for installing the initial restart files from model 
simulation already ran in into the ensemble model directory
'''
import os
import sys
import subprocess
from datetime import datetime, timedelta

sys.path.append('../')
import system_lib as stl

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Config = stl.read_json_dict(config_dir, 'Model.json', 'Assimilation.json',
                            'Initial.json', 'Info.json')
Status = stl.read_json(path=status_path)

assimilation_scheme = Config['Assimilation']['scheme']['name']
model_scheme = Config['Model']['scheme']['name']

system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### *--------------------------------* ###
### *---     start to install     ---* ###

source_dir = os.path.join(Config['Info']['path']['output_path'],
                          Config['Model'][model_scheme]['run_project'],
                          'model_run')
# source_dir='/home/pangmj/Data/pyFilter/projects/ChinaDust20210315/model_run'

iteration_num = Config['Model'][model_scheme]['iteration_num']
Ne = Config['Model'][model_scheme]['ensemble_number']

target_dir = os.path.join(
    Config['Model'][model_scheme]['path']['model_output_path'],
    Config['Model'][model_scheme]['run_project'])
system_log.info('Installing initial files from ' + source_dir)

### *--- install method for typical EnKFs ---* ###
if assimilation_scheme in [
        'enkf', 'enkf_legacy', 'enkf_aod2dust', 'enkf_aod+dust', 'enkf_aod',
        'enkf_dust'
]:

    ### *--- get the time of the installation ---* ###
    time = Config['Initial']['install_restart']['time']
    if time == '':
        time = Config['Initial']['generate_run_time']['equal_step'][
            'first_run_time']

    time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    run_ids = [
        'iter_%02d_ensem_%02d' % (iteration_num, i_ensemble)
        for i_ensemble in range(Ne)
    ]

    ### *--- install the restart files ---* ###
    system_log.debug('installing restart files')
    for i_run in range(len(run_ids)):

        if not os.path.exists(
                os.path.join(target_dir, run_ids[i_run], 'restart')):
            os.makedirs(os.path.join(target_dir, run_ids[i_run], 'restart'))

        source_file = os.path.join(
            source_dir, run_ids[i_run], 'restart', 'LE_%s_state_%s.nc' %
            (run_ids[i_run], time.strftime('%Y%m%d_%H%M')))
        target_file = os.path.join(
            target_dir, run_ids[i_run], 'restart', 'LE_%s_state_%s.nc' %
            (run_ids[i_run], time.strftime('%Y%m%d_%H%M')))

        subprocess.run(['rsync', '-a', source_file, target_file],
                       capture_output=True)
        # print(run_ids[i_run] + ' installed')

    ### *--- install the output files ---* ###
    if assimilation_scheme in ['enkf_aod', 'enkf_aod2dust', 'enkf_aod+dust']:

        system_log.debug('installing output files')
        for i_run in range(len(run_ids)):

            if not os.path.exists(
                    os.path.join(target_dir, run_ids[i_run], 'output')):
                os.makedirs(os.path.join(target_dir, run_ids[i_run], 'output'))

            source_file = os.path.join(
                source_dir, run_ids[i_run], 'output',
                'LE_%s_aod2_%s.nc' % (run_ids[i_run], time.strftime('%Y%m%d')))
            target_file = os.path.join(
                target_dir, run_ids[i_run], 'output',
                'LE_%s_aod2_%s.nc' % (run_ids[i_run], time.strftime('%Y%m%d')))

            subprocess.run(['rsync', '-a', source_file, target_file],
                           capture_output=True)

### *--- install method for NTEnKF ---* ###
elif assimilation_scheme == 'ntenkf_hybrid':

    time = datetime.strptime(Config['Initial']['install_restart']['time'],
                             '%Y-%m-%d %H:%M:%S')

    time_set = Config['Assimilation'][assimilation_scheme]['time_set']
    ensemble_set = Config['Assimilation'][assimilation_scheme]['ensemble_set']

    run_ids = [[
        'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
        for i_ensem in range(int(ensemble_set[i_time]))
    ] for i_time in range(len(time_set))]
    system_log.debug(f'list of run ids : {run_ids}')

    time_points = [
        time + timedelta(hours=int(time_set[i_time]))
        for i_time in range(len(time_set))
    ]

    for i_time in range(len(run_ids)):
        for i_ensemble in range(len(run_ids[i_time])):

            source_tmp = os.path.join(source_dir, run_ids[i_time][i_ensemble],
                                      'restart')
            target_tmp = os.path.join(target_dir, run_ids[i_time][i_ensemble],
                                      'restart')

            if not os.path.exists(target_tmp):
                os.makedirs(target_tmp)

            source_file = source_tmp + '/LE_%s_state_%s.nc' % (
                run_ids[i_time][i_ensemble],
                time_points[i_time].strftime('%Y%m%d_%H%M'))
            target_file = target_tmp + '/LE_%s_state_%s.nc' % (
                run_ids[i_time][i_ensemble],
                time_points[i_time].strftime('%Y%m%d_%H%M'))

            subprocess.run(['rsync', '-a', source_file, target_file],
                           capture_output=True)

system_log.info('Initial files installed into %s' % (target_dir))
