'''
Autor: Mijie Pang
Date: 2022-12-12 12:00:00
LastEditTime: 2024-01-12 09:00:09
Description: This script is written for rsync the source code to the destinated 
working directory and run the PyFilter system there
'''
import os
import sys
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)


def read_json(path: str) -> dict:
    with open(path, 'r') as f:
        load_dict = json.load(f)
    return load_dict


### *--- get basic configurations ---* ###
home_dir = os.getcwd()
pid = os.getpid()

Model = read_json(path='config/Model.json')
Assimilation = read_json(path='config/Assimilation.json')
Info = read_json(path='config/Info.json')

model_scheme = Model['scheme']['name']
assimilation_scheme = Assimilation['scheme']['name']

working_dir = os.path.join(Info['path']['output_path'],
                           Model[model_scheme]['run_project'],
                           Assimilation[assimilation_scheme]['project_name'],
                           'run')

backup_dir = os.path.join(Info['path']['output_path'],
                          Model[model_scheme]['run_project'],
                          Assimilation[assimilation_scheme]['project_name'],
                          'backup_source')

if not os.path.exists(backup_dir):
    os.makedirs(backup_dir)

### *--- prepare working directory ---* ###
logging.info('Working directory : %s' % (working_dir))

if not os.path.exists(working_dir):
    os.makedirs(working_dir)

else:

    command = input(
        'Working directory already exists, clean-write [c], append [a] or ' +
        'abort (any key except [c] and [a]) : ')

    if command == 'c':
        ### check the files and sync the system to the destinated drectory ###
        logging.info('Cleaning the files and rsync the source code')
        command = os.system('rm -r %s/*' % (working_dir))

    elif command == 'a':
        pass

    else:
        logging.info('Unspecified input : %s . Project aborted' % (command))
        sys.exit(1)

exclusion = ','.join([
    'others', '*.pyc', '.gitignore', '__pycache__', 'test*', '*.log', '*.out',
    '.git', 'LICENSE', '*.md', '*.txt', '*.svg', '*.png', 'test/*'
])
command = os.system("rsync -av --exclude={%s} %s/* %s" %
                    (exclusion, home_dir, working_dir))
if command != 0:
    logging.error('create project failed.')
    sys.exit(1)

### *--- backup the source code ---* ###
command = os.system(
    'tar -zcvf %s/backup_source_T%s.tar.gz --exclude=*__pycache__ --exclude=*.out %s'
    % (backup_dir, datetime.now().strftime('%Y%m%d%H%M%S'), working_dir))
if not command == 0:
    logging.error('backup source code failed.')
    sys.exit(1)
else:
    logging.info('source code backuped in %s' % (backup_dir))

### *--- run the sytem project ---* ###
command = input('Run the system [y] ?')
if command == 'y':
    logging.info('Run the PyFilter system under %s' % (working_dir))
    os.chdir(working_dir)
    if Info['System']['forecast_mode']:
        command = os.system('nohup %s start_forecast.py &' %
                            (Info['path']['my_python']))
    else:
        command = os.system('nohup %s start_run.py &' %
                            (Info['path']['my_python']))
else:
    logging.info('System aborted.')
