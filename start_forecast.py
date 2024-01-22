'''
Autor: Mijie Pang
Date: 2023-08-15 20:22:51
LastEditTime: 2023-08-27 20:09:40
Description: 
'''
import os
import sys
import time
import subprocess
from pytz import utc
from datetime import datetime, timedelta

import system_lib as stl

home_dir = os.getcwd()
pid = os.getpid()

config_dir = './config'
status_path = './Status.json'

######################################################
###            configure basic settings            ###

### read configuration from json files ###
Config = stl.read_json_dict(config_dir, get_all=True)
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']

system_project = Config['Model'][model_scheme]['run_project'] + '_' + Config[
    'Assimilation'][assimilation_scheme]['project_name']

stl.edit_json(path=status_path,
              new_dict={
                  'system': {
                      'running': True,
                      'system_project': system_project,
                      'pid': pid,
                      'home_dir': home_dir
                  }
              })

################################################
###                                          ###
###              start PyFilter              ###
###                                          ###
################################################

system_log = stl.Logging(os.path.join(home_dir, system_project + '.log'))
system_log.write(stl.welcome())
system_log.info('PyFilter system started')
system_log.info('PID : %s \n' % (pid))

#####################################################
###          initialization before start          ###
destination = os.path.join(home_dir, 'initial')
system_log.write('### Now I am in %s ###' % (destination))

os.chdir(destination)
command = subprocess.Popen(
    [Config['Info']['path']['my_python'], 'run_initial.py'],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT)
os.chdir(home_dir)

### record the outputs ###
for info in iter(command.stdout.readline, b''):
    system_log.write(bytes.decode(info).strip())

### check the return code ###
command.wait()
if not command.returncode == 0:
    system_log.critical('error happened during initialization')
    sys.exit(-1)
elif command.returncode == 0:
    system_log.info('Initialization finished\n')

### configure the assimilation time ###
initial_time = datetime.now() + timedelta(hours=0)
assimilation_time = datetime(year=initial_time.year,
                             month=initial_time.month,
                             day=initial_time.day,
                             hour=initial_time.hour).astimezone(utc)
assimilation_time_str = assimilation_time.strftime('%Y-%m-%d %H:%M:%S')

#####################################
###      start the main loop      ###
counter = 1
while True:

    ### wait until the time reaches ###
    while datetime.now().astimezone(utc) + timedelta(
            hours=0) < assimilation_time:
        time.sleep(1)

    system_log.write(stl.number_guide(counter))

    model_start_time = assimilation_time
    model_end_time = stl.get_end_time(
        start_time=model_start_time,
        run_time_range=Config['Model'][model_scheme]['running_time_range'])

    ### initialize the status ###
    stl.edit_json(path=status_path,
                  new_dict={
                      'assimilation': {
                          'code': 0,
                          'assimilation_time': assimilation_time,
                          'post_process_code': 0
                      },
                      'model': {
                          'code': 0,
                          'start_time': model_start_time,
                          'end_time': model_end_time,
                          'post_process_code': 0
                      }
                  })

    #################################################
    ###      preparation before assimilation      ###
    destination = os.path.join(home_dir, 'prepare')
    system_log.write('### Now I am in %s ###' % (destination))

    os.chdir(destination)
    command = subprocess.Popen(
        [Config['Info']['path']['my_python'], 'run_prepare.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    os.chdir(home_dir)

    ### record the outputs ###
    for info in iter(command.stdout.readline, b''):
        system_log.write(bytes.decode(info).strip())

    ### check the return code ###
    command.wait()
    if not command.returncode == 0:
        system_log.critical('error happened during preparation')
        sys.exit(-1)
    elif command.returncode == 0:
        system_log.info('Preparation finished\n')

    #######################################################
    ###                                                 ###
    ###                Assimilation Part                ###
    ###                                                 ###
    #######################################################

    system_log.write(
        'Project Name : %s' %
        Config['Assimilation'][assimilation_scheme]['project_name'])
    system_log.write('Assimilation Time : %s' % assimilation_time)

    stl.edit_json(path=status_path,
                  new_dict={
                      'assimilation': {
                          'assimilation_time': assimilation_time,
                          'code': 0
                      }
                  })

    ### run the assimilation analysis ###
    destination = os.path.join(home_dir, 'Assimilation')
    system_log.write('### Now I am in %s ###' % (destination))

    os.chdir(destination)
    command = subprocess.Popen(
        [Config['Info']['path']['my_python'], 'run_assimilation.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    os.chdir(home_dir)

    ### wait until assimilation finish ###
    status = stl.check_status_code('Status.json', 'assimilation', 'code')
    if status < 0:
        system_log.critical('error happened during assimilation')
        sys.exit(status)
    elif status == 100:
        system_log.write('*** End of Assimilation ***\n')

    ### perform post processing after assimilation analysis ###
    destination = os.path.join(home_dir, 'post_process')
    system_log.write('### Now I am in %s ###' % (destination))
    system_log.info('post processing started')

    ### save assimilated files and plot ###
    os.chdir(destination)
    command = subprocess.Popen(
        [Config['Info']['path']['my_python'], 'post_assimilation.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    os.chdir(home_dir)

    ### wait until post-processing finish ###
    status = stl.check_status_code('Status.json', 'assimilation',
                                   'post_process_code')
    if status < 0:
        system_log.error(
            'error happened during post-assimilation processing\n')
    elif status == 100:
        system_log.info('Post processing finished\n')

    ########################################################
    ###                                                  ###
    ###                    Model Part                    ###
    ###                                                  ###
    ########################################################

    ### check run time range to decide if run the model ###
    run_model = True
    if int(Config['Model'][model_scheme]['running_time_range'][:-1]) == 0:
        run_model = False

    if run_model:

        ###########################################
        ###          start to run model         ###
        destination = os.path.join(home_dir, 'Model')
        system_log.write('### Now I am in %s ###' % (destination))
        system_log.write('Model will run from ' + model_start_time + ' to ' +
                         model_end_time)

        ### run the model ###
        os.chdir(destination)
        command = subprocess.Popen(
            [Config['Info']['path']['my_python'], 'run_model.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        os.chdir(home_dir)

        ### wait until model forecast finish ###
        system_log.info('Model forecast started')
        status = stl.check_status_code('Status.json', 'model', 'code')
        if status < 0:
            system_log.critical('error happened during model forecast\n')
            sys.exit(status)
        elif status == 100:
            system_log.info('Model forecast finished\n')

        ##########################################################
        ###    perform post processing after model forecast    ###
        destination = os.path.join(home_dir, 'post_process')
        system_log.write('### Now I am in %s ###' % (destination))
        system_log.info('post processing started')

        ### do model post processing ###
        os.chdir(destination)
        command = subprocess.Popen(
            [Config['Info']['path']['my_python'], 'run_model.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        os.chdir(home_dir)

        ### wait until post-processing finish ###
        status = stl.check_status_code('Status.json', 'model',
                                       'post_process_code')
        if status < 0:
            system_log.error('error happened during model post-processing\n')
            sys.exit(status)
        elif status == 100:
            system_log.info('Post processing finished')

    else:

        system_log.warning('model forecast skipped')
        break

    ### assign the next assimilation time ###
    assimilation_time = stl.get_end_time(
        start_time=assimilation_time,
        run_time_range=Config['Assimilation'][assimilation_scheme]
        ['time_interval'])
    counter += 1
