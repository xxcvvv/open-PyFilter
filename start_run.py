'''
Autor: Mijie Pang
Date: 2023-04-10 16:55:25
LastEditTime: 2024-04-13 09:50:36
Description: This is the whole logical route of the PyFilter system
'''
import os
import sys
import multiprocessing as mp

import initial
import prepare
import Assimilation
import post_asml
import Model
import post_model

import system_lib as stl

home_dir = os.getcwd()
pid = os.getpid()

config_dir = './config'
status_path = './Status.json'

stl.edit_json(
    path=status_path,
    new_dict={'system': {
        'running': True,
        'pid': pid,
        'home_dir': home_dir
    }})

### *--------------------------------------* ###
### *---    configure basic settings    ---* ###

### *--- read configuration from json files ---*###
Config = stl.read_json_dict(config_dir, get_all=True)

model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']
system_project = '%s_%s' % (
    Config['Model'][model_scheme]['run_project'],
    Config['Assimilation'][assimilation_scheme]['project_name'])

Status = stl.edit_json(
    path=status_path,
    new_dict={'system': {
        'system_project': system_project,
    }})

### *--------------------------------------* ###
###                                          ###
###          start PyFilter project          ###
###                                          ###
### *--------------------------------------* ###

system_log = stl.Logging(home_dir, **Config['Info']['System'])
system_log.write(stl.welcome())
system_log.info('PyFilter system started')
system_log.info('PID : %s \n' % (pid))

### *---------------------------------------* ###
### *---   initialization before start   ---* ###
system_log.info('Now I am running Initialization')

initial_process = mp.Process(target=initial.entrance,
                             name='initial',
                             args=(Config, ))
initial_process.start()
initial_process.join()

exit_code = initial_process.exitcode
if exit_code != 0:
    system_log.critical(
        'Something is wrong in initialization! System aborted.')
    sys.exit(exit_code)

### *--- generate the run time list ---* ###
rt = stl.RunTime()
asml_time_list, model_time_list = rt.get(Config['Initial']['run_time'])

### *---------------------------------* ###
### *---    start the main loop    ---* ###

for i_time in range(len(asml_time_list)):

    system_log.write(stl.number_guide(i_time + 1))

    ### initialize the status ###
    Status = stl.edit_json(path=status_path,
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
    system_log.info('Now I am running preparation')

    prepare_process = mp.Process(target=prepare.entrance,
                                 name='prepare',
                                 args=(
                                     Config,
                                     Status,
                                 ))
    prepare_process.start()
    prepare_process.join()

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
        system_log.info('Now I am running Assimilation')

        assimilation_process = mp.Process(target=Assimilation.entrance,
                                          name='Assimilation',
                                          args=(Config, ))
        assimilation_process.start()
        # assimilation_process.join()

        ### *--- wait until assimilation finish ---* ###
        status = stl.check_status_code('Status.json', 'assimilation', 'code')
        if status < 0:
            system_log.critical(
                'ERROR happened during assimilation, system aborted.')
            sys.exit(status)
        elif status == 100:
            system_log.write('*** End of Assimilation ***\n')

        ### *----------------------------------------------------* ###
        ### *---  perform post processing after assimilation  ---* ###
        system_log.info('Now I am running post assimilation process')

        post_asml_process = mp.Process(target=post_asml.entrance,
                                       name='post_asml',
                                       args=(
                                           Config,
                                           Status,
                                       ))
        post_asml_process.start()

    ### *--------------------------------------------* ###
    ###                                                ###
    ###                   Model Part                   ###
    ###                                                ###
    ### *--------------------------------------------* ###

    ### *--- check run time range to decide if run the model ---* ###
    if model_time_list is None:
        system_log.warning('Model forecast skipped')
    else:
        Status = stl.edit_json(path=status_path,
                               new_dict={
                                   'model': {
                                       'start_time':
                                       model_time_list[i_time][0],
                                       'end_time': model_time_list[i_time][1],
                                   }
                               })

        ### *----------------------------------* ###
        ### *---     start to run model     ---* ###
        system_log.write('Now I am running Model')
        system_log.write(
            'Model will run from %s to %s' %
            (model_time_list[i_time][0], model_time_list[i_time][1]))

        model_process = mp.Process(target=Model.entrance,
                                   name='Model',
                                   args=(Config, ))
        model_process.start()
        model_process.join()

        status = stl.check_status_code('Status.json', 'model', 'code')
        if status < 0:
            system_log.critical('ERROR happened during model forecast.\n')
            sys.exit(status)
        elif status == 100:
            system_log.info('Model forecast finished.\n')

        ### *------------------------------------------------------* ###
        ### *---  perform post processing after model forecast  ---* ###
        system_log.info('Now I am running post model process')
        queue = mp.Queue()
        post_asml_process = mp.Process(target=post_model.entrance,
                                       name='post_model',
                                       args=(
                                           Config,
                                           Status,
                                           queue,
                                       ))
        post_asml_process.start()

        while True:
            output = queue.get()
            system_log.info(output)
            if output == 'Please Go On':
                system_log.info(
                    'Post model process is done, moving to the next step.')
                break

### *--- End of the System ---* ###
system_log.info('All work done successfully.')
system_log.write(stl.finished())
stl.edit_json(path=status_path, new_dict={'system': {'running': False}})
