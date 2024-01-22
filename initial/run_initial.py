'''
Autor: Mijie Pang
Date: 2023-07-11 15:43:33
LastEditTime: 2023-12-20 09:24:52
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

model_scheme = Config['Model']['scheme']['name']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

###############################################
###       start to run initialization       ###

if Config['Info']['System']['forecast_mode']:

    command = subprocess.run([
        Config['Info']['path']['my_python'],
    ],
                             capture_output=True)

    sys.exit(0)

if 'install_restart' in Config['Initial']['jobs']:

    script = Config['Script'][model_scheme]['initial']['install_restart']
    if not script == '':

        command = subprocess.run([Config['Info']['path']['my_python'], script],
                                 capture_output=True)

        if not command.returncode == 0:

            system_log.warning(bytes.decode(command.stderr).strip())
            raise RuntimeError('fail to install the restart files')

    else:

        raise ValueError(
            'Installing restart files is not supported in %s yet' %
            (model_scheme))

if 'initial_run' in Config['Initial']['jobs']:

    script = Config['Script'][model_scheme]['initial']['initial_run']
    if not script == '':

        command = subprocess.run([Config['Info']['path']['my_python'], script],
                                 capture_output=True)

        if not command.returncode == 0:

            system_log.warning(bytes.decode(command.stderr).strip())
            raise RuntimeError('fail to run the initial')

    else:

        raise ValueError('Initial model run is not supported in %s yet' %
                         (model_scheme))
