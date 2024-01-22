'''
Autor: Mijie Pang
Date: 2023-04-22 16:07:09
LastEditTime: 2024-01-13 15:12:44
Description: 
'''
import os
import sys
import time
import subprocess
import numpy as np
from datetime import datetime

import Model_lib as mol
import LE_model_lib as leml
import LE_status_lib as lesl

sys.path.append('../')
import system_lib as stl
from tool.node import NodeScript, CheckNode

home_dir = os.getcwd()
pid = os.getpid()

config_dir = '../config'
status_path = '../Status.json'

stl.edit_json(
    path=status_path,
    new_dict={'model': {
        'home_dir': home_dir,
        'code': 10,
        'pid': pid
    }})

### read system configuration ###
Config = stl.read_json_dict(config_dir, 'Model.json', 'Info.json')
Status = stl.read_json(status_path)

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])
check = CheckNode(Config['Info']['Machine']['management'])

### initialize parameters ###
model_scheme = Config['Model']['scheme']['name']
iteration_num = int(Config['Model'][model_scheme]['iteration_num'])
core_demand = int(Config['Model']['node']['core_demand'])
ensemble_num = np.arange(0,
                         int(Config['Model'][model_scheme]['ensemble_number']))
run_ids = [
    'iter_%02d_ensem_%02d' % (iteration_num, i_ensemble)
    for i_ensemble in range(len(ensemble_num))
]

log_trash_dir = Config['Info']['path']['output_path'] + '/log'
if not os.path.exists(log_trash_dir):
    os.makedirs(log_trash_dir)

####################################################################
###                                                              ###
###                 start the parallel model run                 ###
###                                                              ###
####################################################################

### prepare the model sub-directories ###
system_log.info('creating sub-directories')

le_dir_sub = '%s_sub/%s' % (
    Config['Model'][model_scheme]['path']['model_path'],
    Config['Model'][model_scheme]['run_project'])

if not os.path.exists(le_dir_sub):
    os.makedirs(le_dir_sub)
else:
    os.system('rm -rf %s/*' % (le_dir_sub))

### create sub-directories ###
sub_dir = []
for i_ensemble in range(len(ensemble_num)):

    ### rsync the model files to the targeted directory ###
    sub_dir.append(
        mol.copy_from_source(
            model_dir=Config['Model'][model_scheme]['path']['model_path'],
            model_dir_sub=le_dir_sub,
            run_id=run_ids[i_ensemble]))

    ### edit the model configuration ###
    leml.LOTOS_EUROS_Configure_dict(
        project_dir=sub_dir[i_ensemble] + '/proj/dust/002',
        i_ensemble=ensemble_num[i_ensemble],
        config_dict={
            'run.id': run_ids[i_ensemble],
            'run.project': Config['Model'][model_scheme]['run_project'],
            'timerange.start': Status['model']['start_time'],
            'timerange.end': Status['model']['end_time'],
        },
        iteration_num=iteration_num,
        year=Status['model']['start_time'][:4],
        **Config['Model'][model_scheme]['rc'])

system_log.info('sub-directories created')

### run model by time set ###
if Config['Model'][model_scheme]['run_method'] == 'set':

    ### arange node list for submission ###
    node_list = np.zeros(
        [int(Config['Model'][model_scheme]['ensemble_number'])])

    if Config['Model']['node']['auto_node_selection']:

        available_num = check.query(demand=core_demand,
                                    return_type='number_list',
                                    reserve=8)
        node_list = mol.arange_node_list(
            available_num=available_num,
            ensemble_number=Config['Model'][model_scheme]['ensemble_number'],
            core_demand=core_demand,
            **Config['Model']['node'])

    else:
        node_list[:] = int(Config['Model']['node']['node_id'])

    ### submit the model to the node ###
    for i_job in range(len(run_ids)):

        ### prepare submit script ###
        submission = NodeScript(
            path='%s/launcher-server' % (sub_dir[i_job]),
            node_id=node_list[i_job],
            core_demand=core_demand,
            job_name='i_%02d_e_%02d' % (iteration_num, i_job),
            out_file='log/%s.out.log' % (run_ids[i_job]),
            error_file='log/%s.err.log' % (run_ids[i_job]),
            management=Config['Info']['Machine']['management'])
        submission.add(
            'export zijin_dir="/home/pangmj"',
            '. ${zijin_dir}/TNO/env_bash/bashrc_lotos-euros',
            'export LD_LIBRARY_PATH="/home/jinjb/TNO/lotos-euros/v2.1_dust2021_ensemble/tools:$LD_LIBRARY_PATH"',
            './launcher-radiation -s')
        submit_command = submission.get_command()

        ### submit to the node ###
        os.chdir(sub_dir[i_job])
        command = os.system(
            '. ' + Config['Model'][model_scheme]['path']['model_bashrc_path'])
        command = os.system(submit_command)

        system_log.info('%s submitted to node %s' %
                        (run_ids[i_job], node_list[i_job]))

    leml.wait_for_model_paralleml(
        le_dir_sub=le_dir_sub,
        mv_model_log_dir=Config['Info']['path']['output_path'] + '/log',
        run_id=run_ids,
        system_log=system_log)

### run model in batches ###
elif Config['Model'][model_scheme]['run_method'] == 'batch':

    system_log.write('### run model by batch ###')

    run_id_flat = np.ravel(np.array(run_ids))

    # status : 0 for ready, 10 for submitted, 20 for running, 100 for finished
    ensemble_status_dict = {
        run_id_flat[i_run]: {
            'status': 0,
            'run_dir': sub_dir[i_run],
            # 'progress': None
        }
        for i_run in range(len(run_id_flat))
    }

    system_log.debug(ensemble_status_dict.keys())

    node_id = int(Config['Model']['node']['core_demand'])
    occupied_core_num = 1
    retry_count = 0
    while len(ensemble_status_dict) > 0:

        ### loop over all remaining ensembles ###
        delete_index = []

        for i_run, run_id in enumerate(ensemble_status_dict):

            ### *--- stage 1: submit the model to node ---* ###
            flag1 = ensemble_status_dict[run_id]['status'] == 0
            flag2 = occupied_core_num <= Config['Model']['node']['max_core_num']
            if flag1 and flag2:

                if Config['Model']['node']['auto_node_selection']:

                    node_id = check.query(demand=core_demand,
                                          reserve=8,
                                          random_choice=True,
                                          **Config['Model']['node'])

                ### prepare submit script ###
                submission = NodeScript(
                    path='%s/launcher-server' %
                    (ensemble_status_dict[run_id]['run_dir']),
                    node_id=node_id,
                    core_demand=core_demand,
                    job_name='i_%02d_e_%02d' %
                    (iteration_num, int(run_id.split('_')[3])),
                    out_file='log/%s.out.log' % (run_id),
                    error_file='log/%s.err.log' % (run_id),
                    management=Config['Info']['Machine']['management'])
                submission.add(
                    'export zijin_dir="/home/pangmj"',
                    '. ${zijin_dir}/TNO/env_bash/bashrc_lotos-euros_v22',
                    'export LD_LIBRARY_PATH="/home/jinjb/TNO/lotos-euros/v2.1_dust2021_ensemble/tools:$LD_LIBRARY_PATH"',
                    './launcher-radiation -n')
                submit_command = submission.get_command()

                os.chdir(ensemble_status_dict[run_id]['run_dir'])
                command = os.system(
                    '. ' +
                    Config['Model'][model_scheme]['path']['model_bashrc_path'])
                command = os.system(submit_command)

                system_log.info('%s submitted to node %s' % (run_id, node_id))

                # set the submitted status
                occupied_core_num += core_demand
                ensemble_status_dict[run_id]['status'] = 10
                time.sleep(5)

            ### *--- stage 2: make sure the model has started ---* ###
            elif ensemble_status_dict[run_id]['status'] == 10:

                if os.path.exists(
                        '%s/log/%s.out.log' %
                    (ensemble_status_dict[run_id]['run_dir'], run_id)):

                    # set the running status
                    ensemble_status_dict[run_id]['status'] = 20

            ### *--- stage 3: wait for the model to finish and retry when failed ---* ###
            elif ensemble_status_dict[run_id]['status'] == 20:

                log_file_path = '%s/log/%s.out.log' % (
                    ensemble_status_dict[run_id]['run_dir'], run_id)

                # with open(log_file_path, 'r') as f:
                #     lines = f.readlines()

                # Finished_flag, Error_flag = leml.check_model_state(lines)
                Finished_flag, Error_flag = leml.check_model_log(
                    log_path=log_file_path)

                ### if error happened, reset the ensemble to ready mode ###
                if Error_flag:

                    system_log.warning(
                        '%s error happened, reset to ready mode' % (run_id))

                    ### rename and save the log file or just delete it ###
                    log_file_moved = os.path.join(
                        log_trash_dir, '%s$error.%s.log' %
                        (datetime.now().strftime('%Y%m%d_%H%M%S'), run_id))
                    subprocess.run(['mv', log_file_path, log_file_moved])

                    # set the status
                    ensemble_status_dict[run_id]['status'] = 0
                    occupied_core_num -= core_demand
                    retry_count += 1

                ### if it is finished secessfully, release the occupied core
                elif Finished_flag:

                    system_log.info('%s finished successfully' % (run_id))

                    ### rename and save the log file or just delete it ###
                    # log_file_moved = log_trash_dir + '/' + run_ids + '.out$' + datetime.now(
                    # ).strftime('%Y%m%d_%H%M%S') + '.log'
                    # subprocess.run(['mv', log_file_path, log_file_moved])
                    os.remove(log_file_path)

                    ### set the status ###
                    ensemble_status_dict[run_id]['status'] = 100
                    occupied_core_num -= core_demand
                    delete_index.append(run_id)

        ### *--- abort the project if the maximum retry is reached ---* ###
        if retry_count > Config['Model'][model_scheme]['max_retry']:
            system_log.error('maximum retry reached, system aborted')
            sys.exit(-1)

        ### remove the finished ensembles ###
        [ensemble_status_dict.pop(idx) for idx in delete_index]

        ### take a snap ###
        time.sleep(5)

### update the finished status ###
os.chdir(home_dir)
stl.edit_json(path=status_path, new_dict={'model': {'code': 100}})
