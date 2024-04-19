'''
Autor: Mijie Pang
Date: 2023-04-22 19:45:15
LastEditTime: 2024-04-15 08:59:45
Description: 
'''
import os
import sys
import logging
import subprocess
import pandas as pd
from glob import glob
from typing import Dict

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
import system_lib as stl


def safe_run_subprocess(
    cmd: list,
    work_dir: str,
):
    try:
        subprocess.run(cmd, check=True, cwd=work_dir)
        logging.debug(f'Successfully ran command: {cmd}')
    except subprocess.CalledProcessError as e:
        logging.error(f'Error running command: {cmd}. Error: {e}')


def make_sure_dir_exists(dir_path: str):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        logging.debug(f'Directory created: {dir_path}')
    else:
        logging.debug(f'Directory already exists: {dir_path}')


def main(Config: Dict, Status: Dict):

    model_scheme = Config['Model']['scheme']['name']
    assi_scheme = Config['Assimilation']['scheme']['name']
    iteration_num = Config['Model'][model_scheme]['iteration_num']

    time_range = pd.date_range(
        Status['model']['start_time'],
        Status['model']['end_time'],
        freq=Config['Model'][model_scheme]['output_time_interval'])

    target_dir = os.path.join(
        Config['Info']['path']['output_path'],
        Config['Model'][model_scheme]['run_project'],
        Config['Assimilation'][assi_scheme]['project_name'], 'forecast',
        time_range[0].strftime('%Y%m%d_%H%M'), 'forecast_files')
    make_sure_dir_exists(target_dir)
    logging.debug(f'Storing to {target_dir}')

    data_type = Config['Model'][model_scheme]['post_process']['data_type']
    save_tool = Config['Model'][model_scheme]['post_process']['save_tool']
    run_type = Config['Model'][model_scheme]['run_type']

    # arrange run_ids
    assert run_type in ['ensemble', 'ensemble_extend']
    if run_type == 'ensemble':
        ensemble_num = int(Config['Model'][model_scheme]['ensemble_number'])
        run_ids = [
            f'iter_{iteration_num:02d}_ensem_{i:02d}'
            for i in range(ensemble_num)
        ]
    elif run_type == 'ensemble_extend':
        time_set = Config['Assimilation'][assi_scheme]['time_set']
        ensemble_set = [
            int(ens)
            for ens in Config['Assimilation'][assi_scheme]['ensemble_set']
        ]
        run_ids = [[f't_{time}_e_{i:02d}' for i in range(ens)]
                   for time, ens in zip(time_set, ensemble_set)]
        run_ids = [item for sublist in run_ids for item in sublist]

    # save the files
    for run_id in run_ids:

        source_dir = os.path.join(
            Config['Model'][model_scheme]['path']['model_output_path'],
            Config['Model'][model_scheme]['run_project'], run_id)

        if 'output' in data_type:
            source_dir_output = os.path.join(source_dir, 'output')
            target_dir_output = os.path.join(target_dir, run_id, 'output')
            make_sure_dir_exists(target_dir_output)

        if 'restart' in data_type:
            source_dir_restart = os.path.join(source_dir, 'restart')
            target_dir_restart = os.path.join(target_dir, run_id, 'restart')
            make_sure_dir_exists(target_dir_restart)

        output_file_list = []
        for i_time in range(len(time_range)):

            if 'restart' in data_type:
                source_file = os.path.join(
                    source_dir_restart,
                    f'LE_{run_id}_state_{time_range[i_time].strftime("%Y%m%d_%H%M")}.nc'
                )
                if i_time == 0:
                    safe_run_subprocess(
                        ['rsync', '-a', source_file, target_dir_restart],
                        source_dir_restart)
                else:
                    safe_run_subprocess(
                        [save_tool, source_file, target_dir_restart],
                        source_dir_restart)

            if 'output' in data_type:
                output_file_list += glob(
                    os.path.join(
                        source_dir_output,
                        f'LE_{run_id}_conc*_{time_range[i_time].strftime("%Y%m%d")}.nc'
                    ))

        output_file_list = list(set(output_file_list))
        for i_file in range(len(output_file_list)):
            subprocess.run(
                [save_tool, output_file_list[i_file], target_dir_output])

    logging.info('Storation finished')


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    Status = stl.read_json(path=os.path.join(main_dir, 'Status.json'))
    main(Config, Status)
