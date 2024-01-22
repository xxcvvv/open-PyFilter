'''
Autor: Mijie Pang
Date: 2022-12-04 09:20:01
LastEditTime: 2023-09-22 19:18:56
Description: 
'''
import os
import sys

import Model_lib as mol
import LE_model_lib as leml

sys.path.append('../')
import system_lib as stl

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

### read configuration ###
Config = stl.read_json_dict(config_dir, 'Assimilation.json', 'Model.json',
                            'Info.json')
Status = stl.read_json(path=status_path)

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### initialize parameters ###
model_scheme = Config['Model']['scheme']['name']
assi_scheme = Config['Assimilation']['scheme']['name']

iteration_num = int(Config['Model'][model_scheme]['iteration_num'])
run_id = Config['Assimilation'][assi_scheme]['project_name']
system_project = Status['system']['system_project']
le_dir = Config['Model'][model_scheme]['path']['model_path']
le_dir_sub = Config['Model'][model_scheme]['path']['model_path'] + '_sub'

mol.copy_from_source(model_dir=le_dir, model_dir_sub=le_dir_sub, run_id=run_id)

### select node ###
core_demand = int(Config['Model']['node']['core_demand'])
node_id = stl.get_available_node(demand=core_demand)

## configure model ###
leml.LOTOS_EUROS_Configure_dict(
    project_dir=le_dir_sub + '/' + run_id + '/proj/dust/002',
    i_ensemble=0,
    config_dict={
        'run.id': run_id,
        'run.project': Config['Model'][model_scheme]['run_project'],
        'timerange.start': Status['model']['start_time'],
        'timerange.end': Status['model']['end_time'],
        'par.nthread': str(core_demand)
    },
    iteration_num=iteration_num,
    **Config['Model'][model_scheme]['rc'])

mol.launcher_sever_Configure(
    le_dir=le_dir_sub + '/' + run_id,
    run_id=run_id,
    node_id=node_id,
    core_demand=core_demand,
    job_name=run_id,
    text=[
        'export zijin_dir="/home/pangmj"',
        '. ${zijin_dir}/TNO/env_bash/bashrc_lotos-euros',
        'export LD_LIBRARY_PATH="/home/jinjb/TNO/lotos-euros/v2.1_dust2021_ensemble/tools:$LD_LIBRARY_PATH"',
        './launcher-radiation -s'
    ])

### run the model ###
os.chdir(le_dir_sub + '/' + run_id)
command = os.system('. ' +
                    Config['Model'][model_scheme]['path']['model_bashrc_path'])
command = os.system('qsub launcher-server')

leml.wait_for_model(le_dir_sub=le_dir_sub,
                    mv_model_log_dir=Config['Info']['path']['output_path'] +
                    '/log',
                    run_id=Config['Assimilation'][assi_scheme]['project_name'],
                    system_log=system_log)

### update the finished status ###
os.chdir(home_dir)
stl.edit_json(path=status_path, new_dict={'model': {'code': 100}})
