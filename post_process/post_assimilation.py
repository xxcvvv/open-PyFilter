'''
Autor: Mijie Pang
Date: 2023-02-18 09:30:55
LastEditTime: 2024-01-10 19:03:41
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
Config = stl.read_json_dict(config_dir, 'Model.json', 'Info.json',
                            'Assimilation.json')
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### *-------------------------------------------* ###

### *--- plot the results ---* ###
if Config['Assimilation']['post_process']['plot_results']:

    run_srcipt = Config['Script'][model_scheme]['Assimilation'][
        'post_process']['plot']
    if not run_srcipt == '':

        command = subprocess.Popen([
            Config['Info']['path']['my_python'], run_srcipt,
            '-assimilation_time', Status['assimilation']['assimilation_time']
        ],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

        print('continue')

        output, error = command.communicate()
        if not command.returncode == 0:
            system_log.warning(bytes.decode(error).strip())
