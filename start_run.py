'''
Autor: Mijie Pang
Date: 2023-04-10 16:55:25
LastEditTime: 2024-01-11 10:21:30
Description: This is the whole logical route of the PyFilter system
'''
import os
import sys
import signal
import subprocess

import system_lib as stl

home_dir = os.getcwd()
pid = os.getpid()

config_dir = './config'
status_path = './Status.json'


### *--- set a signal handler to
#close the subproces when main process exits ---* ###
def handler(signum, frame):
    command.terminate()


### *--------------------------------------* ###
### *---    configure basic settings    ---* ###

### *--- read configuration from json files ---*###
Config = stl.read_json_dict(config_dir, get_all=True)
Status = stl.read_json(status_path)

model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']
system_project = '%s_%s' % (
    Config['Model'][model_scheme]['run_project'],
    Config['Assimilation'][assimilation_scheme]['project_name'])

stl.edit_json(path=status_path,
              new_dict={
                  'system': {
                      'running': True,
                      'system_project': system_project,
                      'pid': pid,
                      'home_dir': home_dir
                  }
              })

### *--------------------------------------* ###
###                                          ###
###          start PyFilter project          ###
###                                          ###
### *--------------------------------------* ###

system_log = stl.Logging(os.path.join(home_dir, system_project + '.log'))
system_log.write(stl.welcome())
system_log.info('PyFilter system started')
system_log.info('PID : %s \n' % (pid))

### *---------------------------------------* ###
### *---   initialization before start   ---* ###
destination = os.path.join(home_dir, 'initial')
system_log.write('### Now I am in %s ###' % (destination))

os.chdir(destination)
command = subprocess.Popen(
    [Config['Info']['path']['my_python'], '-u', 'run_initial.py'],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT)
os.chdir(home_dir)

### *--- record the outputs ---* ###
signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)
for info in iter(command.stdout.readline, b''):
    system_log.write(bytes.decode(info).strip())

### *--- check the return code ---* ###
command.wait()
if not command.returncode == 0:
    system_log.critical('ERROR happened during initialization.')
    sys.exit(-1)
elif command.returncode == 0:
    system_log.info('Initialization finished.')

### *--- generate the run time list ---* ###
asml_time_list, model_time_list = stl.get_run_time(
    Config['Initial']['generate_run_time'])

### *---------------------------------* ###
### *---    start the main loop    ---* ###

for i_time in range(len(asml_time_list)):

    system_log.write(stl.number_guide(i_time + 1))

    ### initialize the status ###
    stl.edit_json(path=status_path,
                  new_dict={
                      'assimilation': {
                          'code': 0,
                          'assimilation_time': asml_time_list[i_time],
                          'post_process_code': 0
                      },
                      'model': {
                          'code': 0,
                          'post_process_code': 0
                      }
                  })

    ### *-------------------------------------------* ###
    ### *---   preparation before assimilation   ---* ###
    destination = os.path.join(home_dir, 'prepare')
    system_log.write('### Now I am in %s ###' % (destination))

    os.chdir(destination)
    command = subprocess.Popen(
        [Config['Info']['path']['my_python'], '-u', 'run_prepare.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    os.chdir(home_dir)

    ### *--- record the outputs ---* ###
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    for info in iter(command.stdout.readline, b''):
        system_log.write(bytes.decode(info).strip())

    ### *--- check the return code ---* ###
    command.wait()
    if not command.returncode == 0:
        system_log.error('ERROR happened during preparation.')
        sys.exit(-1)
    elif command.returncode == 0:
        system_log.info('Preparation finished.\n')

    ### *---------------------------------------------* ###
    ###                                                 ###
    ###                Assimilation Part                ###
    ###                                                 ###
    ### *---------------------------------------------* ###

    ### *--- decide if run the assimilation ---* ###
    if not Config['Assimilation']['scheme']['enable']:

        system_log.warning('Assimilation skipped.')

    else:

        system_log.write(
            'Project Name : %s' %
            Config['Assimilation'][assimilation_scheme]['project_name'])
        system_log.write('Assimilation Time : %s' % asml_time_list[i_time])

        ### *-------------------------------------* ###
        ### *--- run the assimilation analysis ---* ###
        destination = os.path.join(home_dir, 'Assimilation')
        system_log.write('### Now I am in %s ###' % (destination))

        os.chdir(destination)
        command = subprocess.Popen(
            [Config['Info']['path']['my_python'], '-u', 'run_assimilation.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        os.chdir(home_dir)

        ### *--- wait until assimilation finish ---* ###
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        status = stl.check_status_code('Status.json', 'assimilation', 'code')
        if status < 0:
            system_log.critical(
                'ERROR happened during assimilation, system aborted.')
            sys.exit(status)
        elif status == 100:
            system_log.write('*** End of Assimilation ***\n')

        ### *----------------------------------------------------* ###
        ### *---  perform post processing after assimilation  ---* ###
        destination = os.path.join(home_dir, 'post_process')
        system_log.write('### Now I am in %s ###' % (destination))
        system_log.info('Post processing started')

        ### *--- save assimilated files and plot ---* ###
        os.chdir(destination)
        command = subprocess.Popen([
            Config['Info']['path']['my_python'], '-u', 'post_assimilation.py'
        ],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        os.chdir(home_dir)

        ### *--- wait until post-processing finish ---* ###
        while command.poll() is None:
            output = command.stdout.readline().decode().strip()
            if output == 'continue':
                system_log.info('Post processing finished.\n')
                break

    ### *----------------------------------------------* ###
    ###                                                  ###
    ###                    Model Part                    ###
    ###                                                  ###
    ### *----------------------------------------------* ###

    ### *--- check run time range to decide if run the model ---* ###
    if model_time_list == []:

        system_log.warning('Model forecast skipped')

    else:

        stl.edit_json(path=status_path,
                      new_dict={
                          'model': {
                              'start_time': model_time_list[i_time][0],
                              'end_time': model_time_list[i_time][1],
                          }
                      })

        ### *----------------------------------* ###
        ### *---     start to run model     ---* ###
        destination = os.path.join(home_dir, 'Model')
        system_log.write('### Now I am in %s ###' % (destination))
        system_log.write(
            'Model will run from %s to %s' %
            (model_time_list[i_time][0], model_time_list[i_time][1]))

        ### *--- run the model ---* ###
        os.chdir(destination)
        command = subprocess.Popen(
            [Config['Info']['path']['my_python'], '-u', 'run_model.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        os.chdir(home_dir)

        ### *--- wait until model forecast finish ---* ###
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        status = stl.check_status_code('Status.json', 'model', 'code')
        if status < 0:
            system_log.critical('ERROR happened during model forecast.\n')
            sys.exit(status)
        elif status == 100:
            system_log.info('Model forecast finished.\n')

        ### *------------------------------------------------------* ###
        ### *---  perform post processing after model forecast  ---* ###
        destination = os.path.join(home_dir, 'post_process')
        system_log.write('### Now I am in %s ###' % (destination))
        system_log.info('Post processing started')

        ### *--- do model post processing ---* ###
        os.chdir(destination)
        command = subprocess.Popen(
            [Config['Info']['path']['my_python'], '-u', 'post_model.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        os.chdir(home_dir)

        ### *--- wait until post-processing finish ---* ###
        while command.poll() is None:
            output = command.stdout.readline().decode().strip()
            if output == 'continue':
                system_log.info('Post processing finished.\n')
                break
            elif output == 'terminate':
                system_log.error(
                    'ERROR happened during model post-processing.\n')
                sys.exit(status)
                
        # status = stl.check_status_code('Status.json', 'model',
        #                                'post_process_code')
        # if status < 0:
        #     system_log.error('ERROR happened during model post-processing.\n')
        #     sys.exit(status)
        # elif status == 100:
        #     system_log.info('Post processing finished.')

### *--- End of the System ---* ###
system_log.info('All work done successfully.')
system_log.write(stl.finished())
stl.edit_json(path=status_path, new_dict={'system': {'running': False}})
