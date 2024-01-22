'''
Autor: Mijie Pang
Date: 2023-04-22 19:13:57
LastEditTime: 2024-01-11 10:25:58
Description: 
'''
import os
import sys
import subprocess

sys.path.append('../')
import system_lib as stl
from tool.node import NodeScript, CheckNode

home_dir = os.getcwd()

config_dir = '../config'
status_path = '../Status.json'

################################################
###            read configuration            ###
Config = stl.read_json_dict(config_dir, 'Info.json', 'Assimilation.json')
Status = stl.read_json(path=status_path)

assimilation_scheme = Config['Assimilation']['scheme']['name']
node_id = Config['Assimilation']['node']['node_id']
core_demand = Config['Assimilation']['node']['core_demand']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### tell the main branch that i am started ###
stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 10}})

### *--- Gate Way ---* ###
### run the script on gate ###
if Config['Assimilation']['scheme']['run'] == 'gate':

    system_log.info(
        'Assimilation project -> "%s" <- launched to gate' %
        (Config['Assimilation'][assimilation_scheme]['project_name']))

    ### run the script directly on the gate ###
    command = subprocess.Popen([
        Config['Info']['path']['my_python'], '-u', 'Assimilation_entrance.py'
    ],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)

    for info in iter(command.stdout.readline, b''):
        info_str = bytes.decode(info).strip()
        system_log.debug(info_str)

    command.wait()

    if not command.returncode == 0:
        system_log.warning(bytes.decode(command.stderr).strip())

### *--- Node Way ---* ###
### submit the script to the node ###
elif Config['Assimilation']['scheme']['run'] == 'node':

    ### prepare the job submission script ###
    if Config['Assimilation']['node']['auto_node_selection']:
        check = CheckNode(Config['Info']['Machine']['management'])
        node_id = check.query(demand=core_demand,
                              return_type='str',
                              **Config['Assimilation']['node'])

    submission = NodeScript(
        path='%s/run_assimilation.sh' % (home_dir),
        node_id=node_id,
        core_demand=core_demand,
        job_name=Config['Assimilation'][assimilation_scheme]['project_name'],
        management=Config['Info']['Machine']['management'])
    submission.add(
        'cd %s' % (home_dir), '%s -u Assimilation_entrance.py' %
        (Config['Info']['path']['my_python']))
    submit_command = submission.get_command()

    ### submit to the node ###
    command = subprocess.Popen(submit_command.split(' '),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)

    for info in iter(command.stdout.readline, b''):
        info_str = bytes.decode(info).strip()
        system_log.debug(info_str)

    command.wait()

    ### record the status ###
    system_log.info(
        'Assimilation project -> "%s" <- submitted to %s' %
        (Config['Assimilation'][assimilation_scheme]['project_name'], node_id))
