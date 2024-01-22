'''
Autor: Mijie Pang
Date: 2023-03-25 09:31:43
LastEditTime: 2023-12-10 07:51:59
Description: 
'''
import os
import sys
import subprocess

sys.path.append('../')
import system_lib as stl
from tool.node import NodeScript

home_dir = os.getcwd()

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Config = stl.read_json_dict(config_dir, 'Model.json', 'Info.json',
                            'Script.json')
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### update the status code ###
# stl.edit_json(path=status_path, new_dict={'model': {'post_process_code': 10}})

### *-------------------------------------* ###
### *---    run the post processing    ---* ###

### choice 1: do nothing to the model output ###
if Config['Model'][model_scheme]['post_process']['save_method'] in [
        0, '0', 'none'
]:

    pass

### choice 2: save the original model output to somewhere else ###
elif Config['Model'][model_scheme]['post_process']['save_method'] in [
        1, '1', 'origin', 2, '2', 'merge'
]:

    ### run the script ###
    if 'origin' in Config['Script'][model_scheme]['Model']['post_process'][
            'save'].keys():

        command = subprocess.run([
            Config['Info']['path']['my_python'], Config['Script'][model_scheme]
            ['Model']['post_process']['save']['origin']
        ],
                                 capture_output=True)

        if not command.returncode == 0:

            system_log.warning(bytes.decode(command.stderr).strip())
            print('terminate')
            # stl.edit_json(path=status_path,
            #               new_dict={'model': {
            #                   'post_process_code': -1
            #               }})
            sys.exit(-1)

### after finish the necesssary procedure, the system continus ###
print('continue')
# stl.edit_json(path=status_path, new_dict={'model': {'post_process_code': 100}})

##################################################################
### parts that won't jam the system

### generate the combined product ###
if Config['Model'][model_scheme]['post_process']['save_method'] in [
        2, '2', 'merge'
]:

    command = subprocess.run([
        Config['Info']['path']['my_python'], Config['Script'][model_scheme]
        ['Model']['post_process']['save']['merge']
    ],
                             capture_output=True)

    if not command.returncode == 0:
        system_log.debug(bytes.decode(command.stderr).strip())

### plot the model forecast results ###
if Config['Model'][model_scheme]['post_process']['plot_results'] and not Config[
        'Script'][model_scheme]['Model']['post_process']['plot'] == '':

    run_spec = Config['Model'][model_scheme]['post_process']['run_spec']

    if run_spec[0] == 'gate':

        command = subprocess.run([
            Config['Info']['path']['my_python'],
            Config['Script'][model_scheme]['Model']['post_process']['plot']
        ],
                                 capture_output=True)

        if not command.returncode == 0:
            system_log.debug(bytes.decode(command.stderr).strip())

    elif run_spec[0] == 'node':

        node_id = stl.get_available_node(demand=run_spec[1], return_type='str')

        submission = NodeScript(
            path='%s/qsub_post_process.sh' % (home_dir),
            node_id=node_id,
            core_demand=run_spec[1],
            job_name='post_forecast',
            management=Config['Info']['Machine']['management'])
        submission.add(
            'cd %s' % (home_dir), '%s %s' %
            (Config['Info']['path']['my_python'],
             Config['Script'][model_scheme]['Model']['post_process']['plot']))
        submit_command = submission.get_command()

        command = os.system(submit_command)

### end of part
##################################################################
