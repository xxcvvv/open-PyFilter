'''
Autor: Mijie Pang
Date: 2022-11-29 09:56:19
LastEditTime: 2023-09-15 09:19:00
Description: the first step to start the model 
'''
import os
import sys
import subprocess

sys.path.append('../')
import system_lib as stl

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Config = stl.read_json_dict(config_dir, 'Info.json')
Status = stl.read_json(path=status_path)

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

command = subprocess.run(
    [Config['Info']['path']['my_python'], 'Model_entrance.py'],
    capture_output=True)

if not command.returncode == 0:

    system_log.warning(bytes.decode(command.stderr).strip())
    stl.edit_json(path=status_path, new_dict={'model': {'code': -1}})
