'''
Autor: Mijie Pang
Date: 2023-04-24 19:36:09
LastEditTime: 2023-12-10 07:58:31
Description: guiding to the selected model and ensemble running method
'''
import os
import sys
import subprocess

sys.path.append('../')
import system_lib as stl

config_dir = '../config'
status_path = '../Status.json'

### read configuration from json files ###
Config = stl.read_json_dict(config_dir, 'Model.json', 'Info.json',
                            'Script.json')
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### run the model script ###
command = subprocess.Popen([
    Config['Info']['path']['my_python'], Config['Script'][model_scheme]
    ['Model'][Config['Model'][model_scheme]['run_type']]
],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT)

### record the outputs ###
for info in iter(command.stdout.readline, b''):
    system_log.debug(bytes.decode(info).strip())

command.wait()
if not command.returncode == 0:

    system_log.warning('something is wrong, check the log')
    sys.exit(command.returncode)
