'''
Autor: Mijie Pang
Date: 2023-04-24 19:38:17
LastEditTime: 2024-01-13 15:05:22
Description: 
'''
import os
import time
import subprocess
import numpy as np
from datetime import datetime


def LOTOS_EUROS_Configure(
    le_dir: str,
    i_ensemble: int,
    iteration_num: int,
    run_id: str,
    run_project: str,
    start_time: str,
    end_time: str,
    start_from_restart: str,
    background_flag=False,
) -> None:

    ### this is to write the lotos-euros.rc file ###
    with open('%s/rc/lotos-euros.rc' % (le_dir), 'r') as file:
        # read a list of lines into data
        code = file.readlines()
    for line in np.arange(40, 50):  # approximate range
        if code[line][0:6] == 'run.id':
            code[line] = 'run.id			 :   %s\n' % (run_id)
        if code[line][0:11] == 'run.project':
            code[line] = 'run.project		:   %s\n' % (run_project)

    for line in np.arange(180, 200):
        if code[line].startswith('timerange.start'):
            code[line] = 'timerange.start     :  %s\n' % (start_time)
        if code[line].startswith('timerange.end'):
            code[line] = 'timerange.end       :  %s\n' % (end_time)

    for line in np.arange(610, 620):
        if code[line].startswith('le.restart                    :'):
            code[line] = 'le.restart                    :  %s\n' % (
                start_from_restart)

    # and write everything back
    with open('%s/rc/lotos-euros.rc' % (le_dir), 'w') as file:
        file.writelines(code)

    ### this is to write the le_emis_dust_wind.F90 ###
    with open('%s/src/le_emis_dust_wind.F90' % (le_dir), 'r') as file:
        # read a list of lines into data
        code = file.readlines()

    for line in np.arange(1045, 1085):  ### approximated range
        if code[line][0:23] == "       open(unit=3,file":
            # print('to change le_emis_dust_wind')
            if background_flag:
                code[
                    line] = "       open(unit=3,file='/home/pangmj/Data/pyFilter/emis_beta100/emis_iteration/emis_iter_%02d/emis_background/'&\n" % (
                        iteration_num)
            else:
                code[
                    line] = "       open(unit=3,file='/home/pangmj/Data/pyFilter/emis_beta100/emis_iteration/emis_iter_%02d/emis_ensem_%02d/'&\n" % (
                        iteration_num, i_ensemble)

    # and write everything back
    with open('%s/src/le_emis_dust_wind.F90' % (le_dir), 'w') as file:
        file.writelines(code)


def LOTOS_EUROS_Configure_dict(project_dir: str,
                               config_dict: dict,
                               iteration_num: int,
                               i_ensemble: int,
                               year=2018,
                               background_flag=False,
                               **kwargs) -> None:

    ### *----------------------------------* ###
    ### *---     edit the .rc files     ---* ###

    ### *--- edit the lotos-eiros.rc file ---* ####
    # open the lotos-euros.rc
    with open('%s/rc/lotos-euros.rc' % (project_dir), 'r') as file:
        code = file.readlines()

    ### *--- modify the configure according to the config_dict ---* ###
    for line in range(len(code)):

        if not code[line].startswith(('!', '#')):

            LE_key = code[line].split(' ')[0]
            if LE_key in config_dict.keys():
                code[line] = '%s:  %s\n' % (''.join(
                    code[line].split(':')[0]), config_dict[LE_key])

            if LE_key in kwargs['lotos-euros.rc'].keys():
                code[line] = '%s:  %s\n' % (''.join(code[line].split(':')[0]),
                                            kwargs['lotos-euros.rc'][LE_key])

    # write everything back
    with open('%s/rc/lotos-euros.rc' % (project_dir), 'w') as file:
        file.writelines(code)

    ### *--- edit some other files ---* ###
    for rc_file in kwargs.keys():
        if os.path.exists('%s/rc/%s' % (project_dir, rc_file)):

            with open('%s/rc/%s' % (project_dir, rc_file), 'r') as file:
                code = file.readlines()

            ### *--- modify the configure according to the dict ---* ###
            for line in range(len(code)):

                if not code[line].startswith(('!', '#')):

                    LE_key = code[line].split(' ')[0]
                    if LE_key in kwargs[rc_file].keys():
                        code[line] = '%s:  %s\n' % (''.join(
                            code[line].split(':')[0]), kwargs[rc_file][LE_key])

            # write everything back
            with open('%s/rc/%s' % (project_dir, rc_file), 'w') as file:
                file.writelines(code)

    ### *--------------------------------------* ###
    ### *---      edit the source files     ---* ###

    with open('%s/src/le_emis_dust_wind.F90' % (project_dir), 'r') as file:
        code = file.readlines()

    ### *--- input the pertubated dust emission field ---* ###
    for line in np.arange(1000, 1100):  # approximated range

        if code[line][0:23] == "       open(unit=3,file":
            # print('to change le_emis_dust_wind')
            if background_flag:
                code[
                    line] = "       open(unit=3,file='/home/pangmj/Data/pyFilter/emis_beta100/emis_iteration/emis_iter_%02d/emis_background/'&\n" % (
                        iteration_num)
            else:
                code[
                    line] = "       open(unit=3,file='/home/pangmj/Data/pyFilter/emis_beta100/emis_iteration/emis_iter_%02d/emis_ensem_%02d/'&\n" % (
                        iteration_num, i_ensemble)

        if code[line].startswith("                     //'emis_map_"):
            code[
                line] = "                     //'emis_map_%s_'//time_indice_str//'.csv', action='read')" % (
                    year)
        if code[line].startswith("                     //'emis_length_"):
            code[
                line] = "                     //'emis_length_%s_'//time_indice_str//'.csv', action='read')" % (
                    year)

    # and write everything back
    with open('%s/src/le_emis_dust_wind.F90' % (project_dir), 'w') as file:
        file.writelines(code)


def check_model_state(lines: str, depth=10) -> list:

    Finished_flag = False
    Error_flag = False
    finish_mark = ('[INFO    ] End of script at', '[INFO    ] ** end **',
                   '*** end of simulation reached')
    error_mark = ('[ERROR   ] exception', 'subprocess.CalledProcessError:')

    for line in lines[int(-1 * depth):]:

        if line.startswith(finish_mark):
            Finished_flag = True
        elif line.startswith(error_mark):
            Error_flag = True

    return Finished_flag, Error_flag


def check_model_log(log_path: str, depth=10) -> list:

    Finished_flag = False
    Error_flag = False
    finish_mark = ('[INFO    ] End of script at', '[INFO    ] ** end **',
                   '*** end of simulation reached')
    error_mark = ('[ERROR   ] exception', 'subprocess.CalledProcessError:')

    result = subprocess.run(['tail', '-n', str(depth), log_path],
                            capture_output=True,
                            text=True)
    output = result.stdout.split('\n')

    for line in output:
        if line.startswith(finish_mark):
            Finished_flag = True
        elif line.startswith(error_mark):
            Error_flag = True

    return Finished_flag, Error_flag


def model_resubmit(run_id: str, sub_dir: str) -> str:

    os.chdir(sub_dir)
    os.system('qsub launcher-server')

    return (run_id + ' resubmitted')


### wait for the single model to finish ###
def wait_for_model(le_path: str,
                   run_id: str,
                   mv_model_log_dir='',
                   system_log=None):

    model_log_dir = le_path + '_sub/' + run_id + '/log'

    ####################################
    ### To check if the model starts ###
    start_flag = False
    while not start_flag:

        log_file = os.listdir(model_log_dir)
        if not log_file == []:
            log_file = log_file[0]
            start_flag = True
        else:
            time.sleep(1)

    system_log.info('%s has started' % (run_id))

    ### To check if the model finished ###
    Finished_flag = False
    Error_flag = False
    while not Finished_flag:

        with open(model_log_dir + '/' + log_file, 'r') as f:
            lines = f.readlines()

        Finished_flag, Error_flag = check_model_state(lines)

        if Error_flag:

            system_log.error('%s error happened, retrying' % (run_id))

            ### rename and save the log file or just delete it ###
            if not mv_model_log_dir == '':

                if not os.path.exists(mv_model_log_dir):
                    os.makedirs(mv_model_log_dir)

                log_file_moved = os.path.join(
                    mv_model_log_dir, '%s.error.%s.log' %
                    (log_file[:-8],
                     datetime.now().strftime('$%Y%m%d_%H%M%S') + '.log'))
                subprocess.run(
                    ['mv', model_log_dir + '/' + log_file, log_file_moved])

            else:
                os.remove(model_log_dir + '/' + log_file)

            ### re-submit the model ###
            subprocess.run(['python3', 'LE_retry.py', run_id])

        elif Finished_flag:

            system_log.info('%s finished successfully' % (run_id))

            ### rename and save the log file or just delete it ###
            if not mv_model_log_dir == '':

                if not os.path.exists(mv_model_log_dir):
                    os.makedirs(mv_model_log_dir)

                log_file_moved = os.path.join(
                    mv_model_log_dir, '%s.%s.log' %
                    (log_file[:-4], datetime.now().strftime('%Y%m%d_%H%M%S')))
                subprocess.run(['mv', log_file, log_file_moved])

            else:
                os.remove(model_log_dir + '/' + log_file)

        time.sleep(30)


### wait for the ensemble model to finish ###
def wait_for_model_parallel(le_dir_sub: str,
                            run_id: list,
                            mv_model_log_dir='',
                            system_log=None):

    sub_dirs = [le_dir_sub + '/' + run_id[i_id] for i_id in range(len(run_id))]
    log_dirs = [sub_dir + '/log' for sub_dir in sub_dirs]

    ########################################
    ### To check if all the model starts ###
    log_files = ['mark']
    while len(log_files) < len(sub_dirs):

        log_files = []
        for i in range(len(run_id)):
            if os.path.exists(log_dirs[i] + '/' + run_id[i] + '.out.log'):
                log_files.append(log_dirs[i] + '/' +
                                 os.listdir(log_dirs[i])[0])

        time.sleep(1)

    system_log.info('%s ensembles have started' % (len(sub_dirs)))

    #################################################
    ### To check if the models finished or failed ###
    model_finish_flag = [False for i_id in range(len(run_id))]

    while np.sum(model_finish_flag) < len(run_id):

        ### check every model to see if it finishes ###
        for i_model in range(len(model_finish_flag)):

            if not model_finish_flag[i_model]:

                log_file_path = log_dirs[i_model] + '/' + run_id[
                    i_model] + '.out.log'
                with open(log_file_path, 'r') as f:
                    lines = f.readlines()

                Finished_flag, Error_flag = check_model_state(lines)

                if Error_flag:

                    system_log.error('%s error happened, retrying' %
                                     (run_id[i_model]))

                    ### rename and save the log file or just delete it ###
                    if not mv_model_log_dir == '':

                        if not os.path.exists(mv_model_log_dir):
                            os.makedirs(mv_model_log_dir)

                        log_file_moved = os.path.join(
                            mv_model_log_dir, '%s.error$%s.log' %
                            (run_id[i_model],
                             datetime.now().strftime('%Y%m%d_%H%M%S')))
                        subprocess.run(['mv', log_file_path, log_file_moved])

                    else:
                        os.remove(log_file_path)

                    model_resubmit(run_id=run_id[i_model],
                                   sub_dir=sub_dirs[i_model])

                elif Finished_flag:

                    model_finish_flag[i_model] = True
                    system_log.info('%s finished successfully' %
                                    (run_id[i_model]))

                    ### rename and save the log file or just delete it ###
                    if not mv_model_log_dir == '':

                        if not os.path.exists(mv_model_log_dir):
                            os.makedirs(mv_model_log_dir)

                        log_file_moved = os.path.join(
                            mv_model_log_dir, '%s.out$%s.log' %
                            (run_id[i_model],
                             datetime.now().strftime('%Y%m%d_%H%M%S')))
                        subprocess.run(['mv', log_file_path, log_file_moved])

                    else:
                        os.remove(log_file_path)

        if np.sum(model_finish_flag) < len(run_id):
            time.sleep(30)
