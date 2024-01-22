'''
Autor: Mijie Pang
Date: 2023-04-22 19:10:50
LastEditTime: 2023-12-20 15:01:38
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

sys.path.append('../')
import system_lib as stl
from tool.node import NodeScript

### initialization ###
home_dir = os.getcwd()
pid = os.getpid()
config_dir = '../config'
status_path = '../Status.json'

################################################
###            read configuration            ###

stl.edit_json(
    path=status_path,
    new_dict={'model': {
        'home_dir': home_dir,
        'code': 10,
        'pid': pid
    }})

### read configuration ###
Config = stl.read_json_dict(config_dir, 'Assimilation.json', 'Model.json',
                            'Info.json')
Status = stl.read_json(path=status_path)

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

assi_scheme = Config['Assimilation']['scheme']['name']
model_scheme = Config['Model']['scheme']['name']

core_demand = int(Config['Model']['node']['core_demand'])
time_set = Config['Assimilation'][assi_scheme]['time_set']
ensemble_set = Config['Assimilation'][assi_scheme]['ensemble_set']
run_ids = [[
    't_%s_e_%02d' % (time_set[i_time], i_ensem)
    for i_ensem in range(int(ensemble_set[i_time]))
] for i_time in range(len(time_set))]

log_trash_dir = Config['Info']['path']['output_path'] + '/log'
if not os.path.exists(log_trash_dir):
    os.makedirs(log_trash_dir)

############################################
###     start the ensemble model run     ###
############################################

### prepare all the ensemble model scripts ###
sub_dir = []
run_count = 0
system_log.info('creating sub-directories')
for i_time in range(len(run_ids)):

    ### create model sub directories ###
    for i_ensem in range(len(run_ids[i_time])):

        sub_dir.append(
            mol.copy_from_source(
                model_dir=Config['Model'][model_scheme]['path']['model_path'],
                model_dir_sub=Config['Model'][model_scheme]['path']
                ['model_path'] + '_sub/' +
                Config['Model'][model_scheme]['run_project'],
                run_id=run_ids[i_time][i_ensem]))

    ### configure the model ###
    for i_ensem in range(len(run_ids[i_time])):

        leml.LOTOS_EUROS_Configure_dict(
            project_dir=sub_dir[run_count] + '/proj/dust/002',
            i_ensemble=i_ensem,
            config_dict={
                'run.id': run_ids[i_time][i_ensem],
                'run.project': Config['Model'][model_scheme]['run_project'],
                'timerange.start': Status['model']['start_time'],
                'timerange.end': Status['model']['end_time'],
            },
            iteration_num=int(Config['Model'][model_scheme]['iteration_num']),
            **Config['Model'][model_scheme]['rc'])

        run_count += 1

system_log.info('sub-directories created')

### run model by time set ###
if Config['Model'][model_scheme]['run_method'] == 'set':

    system_log.write('### run model by set ###')
    run_count = 0
    for i_time in range(len(run_ids)):

        ### arrange node list ###
        os.chdir(home_dir)
        node_list = np.zeros([len(run_ids[i_time])])

        if Config['Model']['node']['auto_node_selection']:

            available_num = stl.get_available_node(demand=core_demand,
                                                   reserve=0,
                                                   return_type='number_list')
            node_list = mol.arange_node_list(available_num=available_num,
                                             ensemble_number=len(
                                                 run_ids[i_time]),
                                             core_demand=core_demand)

        else:
            node_list[:] = int(Config['Model']['node']['node_id'])

        ### generate the submission script and run the ensemble model ###
        for i_ensem in range(len(run_ids[i_time])):

            submission = NodeScript(
                path='%s/launcher-server' % (sub_dir[run_count]),
                node_id=node_list[i_ensem],
                core_demand=core_demand,
                job_name=run_ids[i_time][i_ensem],
                out_file='log/%s.out.log' % (run_ids[i_time][i_ensem]),
                error_file='log/%s.err.log' % (run_ids[i_time][i_ensem]),
                management=Config['Info']['Machine']['management'])
            submission.add(
                'export zijin_dir="/home/pangmj"',
                '. ${zijin_dir}/TNO/env_bash/bashrc_lotos-euros',
                'export LD_LIBRARY_PATH="/home/jinjb/TNO/lotos-euros/v2.1_dust2021_ensemble/tools:$LD_LIBRARY_PATH"',
                './launcher-radiation -n')
            submit_command = submission.get_command()

            os.chdir(sub_dir[run_count])
            command = os.system(
                '. ' +
                Config['Model'][model_scheme]['path']['model_bashrc_path'])
            command = os.system(submit_command)

            system_log.info('%s submitted to node %s' %
                            (run_ids[i_time][i_ensem], node_list[i_ensem]))

            run_count += 1

        ### wait for one set ot finish ###
        leml.wait_for_model_paralleml(
            le_dir_sub=Config['Model'][model_scheme]['path']['model_path'] +
            '_sub/' + Config['Model'][model_scheme]['run_project'],
            mv_model_log_dir=Config['Info']['path']['output_path'] + '/log',
            run_id=run_ids[i_time],
            system_log=system_log)

### test feature: run model in batches ###
elif Config['Model'][model_scheme]['run_method'] == 'batch':

    system_log.write('### run model by batch ###')

    run_id_flat = np.ravel(np.array(run_ids))
    ensemble_status_dict = {
        run_id_flat[i_run]: [0, sub_dir[i_run]]
        for i_run in range(len(run_id_flat))
    }
    # 0 for ready, 10 for submitted, 20 for running, 100 for finished

    node_id = int(Config['Model']['node']['core_demand'])
    occupied_core_num = 1
    retry_count = 0

    while len(ensemble_status_dict) > 0:

        ### loop over all remaining ensembles ###
        delete_index = []
        # print(ensemble_status_dict.keys())
        for i_run, run_id in enumerate(ensemble_status_dict):

            ### stage 1: submit the model to node ###
            if ensemble_status_dict[run_id][0] == 0 and \
                occupied_core_num <= Config['Model']['node']['max_core_num']:

                if Config['Model']['node']['auto_node_selection']:
                    node_id = stl.get_available_node(demand=core_demand,
                                                     reserve=8,
                                                     random_choice=True)

                ### prepare the node submission ###
                submission = NodeScript(
                    path='%s/launcher-server' %
                    (ensemble_status_dict[run_id][1]),
                    node_id=node_id,
                    core_demand=core_demand,
                    job_name=run_id,
                    out_file='log/%s.out.log' % (run_ids[i_time][i_ensem]),
                    error_file='log/%s.err.log' % (run_ids[i_time][i_ensem]),
                    management=Config['Info']['Machine']['management'])
                submission.add(
                    'export zijin_dir="/home/pangmj"',
                    '. ${zijin_dir}/TNO/env_bash/bashrc_lotos-euros',
                    'export LD_LIBRARY_PATH="/home/jinjb/TNO/lotos-euros/v2.1_dust2021_ensemble/tools:$LD_LIBRARY_PATH"',
                    './launcher-radiation -n')
                submit_command = submission.get_command()

                ### submit to the node ###
                os.chdir(ensemble_status_dict[run_id][1])
                command = os.system(
                    '. ' +
                    Config['Model'][model_scheme]['path']['model_bashrc_path'])
                command = os.system(submit_command)

                system_log.info('%s submitted to node %s' % (run_id, node_id))

                occupied_core_num += core_demand
                ensemble_status_dict[run_id][
                    0] = 10  # set the submitted status
                time.sleep(5)

            ### stage 2: make sure the model has started ###
            elif ensemble_status_dict[run_id][0] == 10:

                if os.path.exists(ensemble_status_dict[run_id][1] + '/log/' +
                                  run_id + '.out.log'):

                    ensemble_status_dict[run_id][
                        0] = 20  # set the running status

            ### stage 3: wait for the model to finish and retry when failed ###
            elif ensemble_status_dict[run_id][0] == 20:

                log_file_path = ensemble_status_dict[run_id][
                    1] + '/log/' + run_id + '.out.log'

                with open(log_file_path, 'r') as f:
                    lines = f.readlines()

                Finished_flag, Error_flag = leml.check_model_state(lines)

                if Error_flag:

                    system_log.error('%s error happened, reset to ready mode' %
                                     (run_id))

                    ### rename and save the log file or just delete it ###
                    log_file_moved = log_trash_dir + '/' + run_id + '.error$' + datetime.now(
                    ).strftime('%Y%m%d_%H%M%S') + '.log'
                    subprocess.run(['mv', log_file_path, log_file_moved])

                    ### set the status ###
                    ensemble_status_dict[run_id][0] = 0
                    occupied_core_num -= core_demand
                    retry_count += 1

                elif Finished_flag:

                    system_log.info('%s finished successfully' % (run_id))

                    ### rename and save the log file or just delete it ###
                    # log_file_moved = log_trash_dir + '/' + run_id + '.out$' + datetime.now(
                    # ).strftime('%Y%m%d_%H%M%S') + '.log'
                    # subprocess.run(['mv', log_file_path, log_file_moved])
                    os.remove(log_file_path)

                    ### set the status ###
                    ensemble_status_dict[run_id][0] = 100
                    occupied_core_num -= core_demand
                    delete_index.append(run_id)

        ### abort the project when the maximum retry is reached ###
        if retry_count > Config['Model'][model_scheme]['max_retry']:
            system_log.error('maximum retry reached, system aborted')
            sys.exit(1)

        ### remove the finished ensembles ###
        [ensemble_status_dict.pop(idx) for idx in delete_index]

        ### take a snap ###
        time.sleep(5)

### update the finished status ###
os.chdir(home_dir)
stl.edit_json(path=status_path, new_dict={'model': {'code': 100}})
