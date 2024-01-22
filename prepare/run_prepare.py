'''
Autor: Mijie Pang
Date: 2023-07-20 14:54:29
LastEditTime: 2023-12-24 09:03:24
Description: 
'''
import os
import sys
import subprocess

sys.path.append('../')
import system_lib as stl

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Config = stl.read_json_dict(config_dir, get_all=True)
Status = stl.read_json(path=status_path)

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

###################################################
###              start preparation              ###

if Config['Prepare']['jobs'] == []:
    system_log.warning('nothing to do')
    sys.exit()

### *--- download the data needed ---* ###
if 'obs_download' in Config['Prepare']['jobs']:

    command = subprocess.run([
        Config['Info']['path']['my_python'],
        Config['Script']['observation']['download']
    ],
                             capture_output=True)

    if not command.returncode == 0:

        system_log.warning(bytes.decode(command.stderr).strip())
        system_log.warning('fail to download the observations')
        raise RuntimeError()

### *--- download the input data ---* ###
if 'input_download' in Config['Prepare']['jobs']:

    command = subprocess.run([
        Config['Info']['path']['my_python'],
        Config['Script']['input']['download']
    ],
                             capture_output=True)

    if not command.returncode == 0:

        system_log.warning(bytes.decode(command.stderr).strip())
        system_log.warning('fail to download the inputs')
        raise RuntimeError()
