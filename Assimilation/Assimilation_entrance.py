'''
Autor: Mijie Pang
Date: 2023-02-16 16:34:46
LastEditTime: 2024-01-11 10:25:46
Description: 
'''
import os
import sys
import signal
import subprocess

sys.path.append('../')
import system_lib as stl

config_dir = '../config'
status_path = '../Status.json'


### *--- set a signal handler to
#close the subproces when main process exits ---* ###
def handler(signum, frame):
    command.terminate()


### *--- read configuration from json files ---* ###
Config = stl.read_json_dict(config_dir, 'Model.json', 'Info.json',
                            'Script.json', 'Assimilation.json')
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### *---------------------------------------* ###
### *---   run the assimilation script   ---* ###
command = subprocess.Popen([
    Config['Info']['path']['my_python'], '-u',
    Config['Script'][model_scheme]['Assimilation'][assimilation_scheme]
],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT)

signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)

### *--- record the outputs ---* ###
for info in iter(command.stdout.readline, b''):
    system_log.write(bytes.decode(info).strip())

### *--- end of the process ---* ###
command.wait()
if not command.returncode == 0:
    system_log.error('Something is wrong, check the log.')
    stl.edit_json(path=status_path, new_dict={'assimilation': {'code': -1}})
    sys.exit(1)
