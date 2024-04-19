'''
Autor: Mijie Pang
Date: 2024-03-23 10:35:26
LastEditTime: 2024-04-04 17:41:43
Description: Description: designed for installing the initial restart files from model 
simulation already ran in into the ensemble model directory
'''
import os
import sys
import logging
import subprocess
from datetime import datetime, timedelta

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
import system_lib as stl

### *--------------------------------* ###
### *---     start to install     ---* ###


### *--- install method for typical EnKFs ---* ###
def intsll_ordinary(Config: dict, **kwargs) -> None:

    assimilation_scheme = Config['Assimilation']['scheme']['name']
    model_scheme = Config['Model']['scheme']['name']

    iteration_num = Config['Model'][model_scheme]['iteration_num']
    Ne = Config['Model'][model_scheme]['ensemble_number']

    source_dir = os.path.join(Config['Info']['path']['output_path'],
                              Config['Model'][model_scheme]['run_project'],
                              'model_run')

    target_dir = os.path.join(
        Config['Model'][model_scheme]['path']['model_output_path'],
        Config['Model'][model_scheme]['run_project'])
    logging.info('Installing initial files from ' + source_dir)

    ### *--- get the time of the installation ---* ###
    time = Config['Initial']['install_restart']['time']
    if time == '':
        time = Config['Initial']['run_time']['equal_step']['first_run_time']

    time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    run_ids = [
        'iter_%02d_ensem_%02d' % (iteration_num, i_ensemble)
        for i_ensemble in range(Ne)
    ]

    ### *--- install the restart files ---* ###
    logging.debug('Installing restart files')
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

        logging.debug('Installing output files')
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

    logging.info('Initial files installed into %s' % (target_dir))


### *--- install method for NTEnKF ---* ###
def install_neighbor(Config: dict, **kwargs) -> None:

    assimilation_scheme = Config['Assimilation']['scheme']['name']
    model_scheme = Config['Model']['scheme']['name']

    iteration_num = Config['Model'][model_scheme]['iteration_num']

    source_dir = os.path.join(Config['Info']['path']['output_path'],
                              Config['Model'][model_scheme]['run_project'],
                              'model_run')

    target_dir = os.path.join(
        Config['Model'][model_scheme]['path']['model_output_path'],
        Config['Model'][model_scheme]['run_project'])
    logging.info('Installing initial files from ' + source_dir)

    time = Config['Initial']['install_restart']['time']
    if time == '':
        time = Config['Initial']['run_time']['equal_step']['first_run_time']
    time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')

    time_set = Config['Assimilation'][assimilation_scheme]['time_set']
    ensemble_set = Config['Assimilation'][assimilation_scheme]['ensemble_set']

    if assimilation_scheme == 'ntenkf_hybrid':
        run_ids = [[
            'iter_%02d_ensem_%02d' % (iteration_num, i_ensem)
            for i_ensem in range(int(ensemble_set[i_time]))
        ] for i_time in range(len(time_set))]

    elif assimilation_scheme == 'ntenkf_small':
        run_ids = [[
            'iter_%02d_ensem_%02d' %
            (iteration_num, sum(ensemble_set[:i_time]) + i_ensem)
            for i_ensem in range(ensemble_set[i_time])
        ] for i_time in range(len(time_set))]

    logging.debug(f'list of run ids : {run_ids}')

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

    logging.info('Initial files installed into %s' % (target_dir))


def main(Config, **kwargs) -> None:

    logging.info(__file__)
    assimilation_scheme = Config['Assimilation']['scheme']['name']

    method_dict = {
        'enkf': intsll_ordinary,
        'enkf_legacy': intsll_ordinary,
        'enkf_aod2dust': intsll_ordinary,
        'enkf_aod+dust': intsll_ordinary,
        'enkf_aod': intsll_ordinary,
        'enkf_dust': intsll_ordinary,
        'ntenkf_hybrid': install_neighbor,
        'ntenkf_small': install_neighbor
    }

    if assimilation_scheme in method_dict.keys():
        method_dict[assimilation_scheme](Config, **kwargs)
    else:
        logging.warning('Nothing to do here!!!')


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    main(Config)
