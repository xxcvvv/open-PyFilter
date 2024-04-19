'''
Autor: Mijie Pang
Date: 2022-12-04 09:20:01
LastEditTime: 2024-04-05 15:35:07
Description: 
'''
import os
import sys
import time
import logging

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
sys.path.append(os.path.join(main_dir, 'Model'))

import system_lib as stl
from tool.node import NodeScript, CheckNode

import Model_lib as mol
import LE_model_lib as leml
import LE_status_lib as lesl


def main(Config: dict, **kwargs):

    home_dir = os.getcwd()
    pid = os.getpid()
    Status = stl.edit_json(
        path=os.path.join(main_dir, 'Status.json'),
        new_dict={'model': {
            'home_dir': home_dir,
            'code': 10,
            'pid': pid
        }})

    check_node = CheckNode(Config['Info']['Machine']['management'])

    ### *--- initialize parameters ---* ###
    model_scheme = Config['Model']['scheme']['name']
    assi_scheme = Config['Assimilation']['scheme']['name']

    iteration_num = Config['Model'][model_scheme]['iteration_num']
    run_id = Config['Assimilation'][assi_scheme]['project_name']
    le_dir = Config['Model'][model_scheme]['path']['model_path']
    le_dir_sub = Config['Model'][model_scheme]['path']['model_path'] + '_sub'

    mol.copy_from_source(model_dir=le_dir,
                         model_dir_sub=le_dir_sub,
                         run_id=run_id)

    ### *--- edit the model configuration ---* ###
    leml.LOTOS_EUROS_Configure_dict(
        # project_dir=sub_dirs[i_ensemble] + '/proj/dust/002',
        project_dir=le_dir_sub + '/proj/radiation/001',
        config_dict={
            'run.id': run_id,
            'run.project': Config['Model'][model_scheme]['run_project'],
            'timerange.start': Status['model']['start_time'],
            'timerange.end': Status['model']['end_time'],
        },
        **Config['Model'][model_scheme]['rc'])

    ### *--- edit the dust emission file ---* ###
    leml.LOTOS_EUROS_dust_emis(
        # project_dir=sub_dirs[run_count] + '/proj/dust/002',
        project_dir=le_dir_sub + '/proj/radiation/001',
        iteration_num=iteration_num,
        i_ensemble=0,
        # year=start_time[:4],
    )

    ### *--- add the meteo configuration ---* ###
    if Config['Model'][model_scheme]['meteo']['type'] == 'ensem':
        leml.LOTOS_EUROS_meteo_config(base_dir=le_dir_sub + '/base/001',
                                      meteo_num=1)

    ### *--- select node ---* ###
    core_demand = Config['Model']['node']['core_demand']
    node_id = int(Config['Model']['node']['node_id'])
    if Config['Model']['node']['auto_node_selection']:
        node_id = check_node.query(demand=core_demand,
                                   random_choice=True,
                                   **Config['Model']['node'])

    ### *--- prepare submit script ---* ###
    submission = NodeScript(path='%s/launcher-server' % (le_dir_sub),
                            node_id=node_id,
                            core_demand=core_demand,
                            job_name=run_id,
                            out_file='log/%s.out.log' % (run_id),
                            error_file='log/%s.err.log' % (run_id),
                            management=Config['Info']['Machine']['management'])
    submission.add(
        'export zijin_dir="/home/pangmj"',
        '. ' + Config['Model'][model_scheme]['path']['model_bashrc_path'],
        'export LD_LIBRARY_PATH="/home/jinjb/TNO/lotos-euros/v2.1_dust2021_ensemble/tools:$LD_LIBRARY_PATH"',
        './launcher-radiation -n')
    submit_command = submission.get_command()

    ### *--- run the model ---* ###
    os.chdir(le_dir_sub)
    command = os.system(
        '. ' + Config['Model'][model_scheme]['path']['model_bashrc_path'])
    command = os.system(submit_command)

    logging.info('%s submitted to node %s' % (run_id, node_id))

    log_file_path = '%s/log/%s.out.log' % (le_dir_sub, run_id)
    Finished_flag, Error_flag = False, False
    while not Finished_flag or not Error_flag:

        Finished_flag, Error_flag = lesl.check_model_log(
            log_path=log_file_path)

        ### *--- ERROR process ---* ###
        # if error happened, reset the ensemble to ready mode
        if Error_flag:
            logging.warning('%s error happened' % (run_id))

        ### *--- SUCCESS process ---* ###
        # if it is finished secessfully, release the occupied core
        elif Finished_flag:
            logging.info('%s finished successfully' % (run_id))

        time.sleep(5)

    ### *--- update the finished status ---* ###
    os.chdir(home_dir)
    stl.edit_json(path=os.path.join(main_dir, 'Status.json'),
                  new_dict={'model': {
                      'code': 100
                  }})


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    main(Config)
