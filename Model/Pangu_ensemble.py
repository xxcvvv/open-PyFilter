'''
Autor: Mijie Pang
Date: 2023-08-29 16:19:12
LastEditTime: 2023-09-15 09:18:57
Description: 
'''
import os
import sys
import subprocess

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

### read system configuration ###
Config = stl.read_json_dict(config_dir, get_all=True)
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### run the script ###
# use the mpi #
if Config['Model'][model_scheme]['parallel_mode'] == 'mpi':

    # calculate the number of processes #
    process_num = int(Config['Model']['node']['max_core_num'] /
                      Config['Model']['node']['core_demand'])
    if process_num == 0:
        system_log.error('insufficient cores given')
        sys.exit(-1)

    command = subprocess.run([
        'mpirun', '-n',
        str(process_num), Config['Info']['path']['my_python'],
        'Pangu_ensemble_mpi.py'
    ],
                             capture_output=True)

# use the multiprocess #
elif Config['Model'][model_scheme]['parallel_mode'] == 'mp':

    command = subprocess.run(
        [Config['Info']['path']['my_python'], 'Pangu_ensemble_mp.py'],
        capture_output=True)

# check the return code #
if not command.returncode == 0:
    system_log.warning(bytes.decode(command.stderr).strip())
