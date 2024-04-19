'''
Autor: Mijie Pang
Date: 2023-04-24 19:38:17
LastEditTime: 2024-04-18 20:14:31
Description: 
'''
import os
import numpy as np


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


def LOTOS_EUROS_Configure_dict(
    project_dir: str,
    config_dict: dict,
    #    iteration_num: int,
    #    i_ensemble: int,
    #    year=2018,
    #    background_flag=False,
    **kwargs
) -> None:

    ### *----------------------------------* ###
    ### *---     edit the .rc files     ---* ###

    ### *--- edit the lotos-euros.rc file ---* ####
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

    # ### *--------------------------------------* ###
    # ### *---      edit the source files     ---* ###

    # with open('%s/src/le_emis_dust_wind.F90' % (project_dir), 'r') as file:
    #     code = file.readlines()

    # ### *--- input the pertubated dust emission field ---* ###
    # for line in np.arange(1000, 1100):  # approximated range

    #     if code[line][0:23] == "       open(unit=3,file":
    #         # print('to change le_emis_dust_wind')
    #         if background_flag:
    #             code[
    #                 line] = "       open(unit=3,file='/home/pangmj/Data/pyFilter/emis_beta100/emis_iteration/emis_iter_%02d/emis_background/'&\n" % (
    #                     iteration_num)
    #         else:
    #             code[
    #                 line] = "       open(unit=3,file='/home/pangmj/Data/pyFilter/emis_beta100/emis_iteration/emis_iter_%02d/emis_ensem_%02d/'&\n" % (
    #                     iteration_num, i_ensemble)

    #     if code[line].startswith("                     //'emis_map_"):
    #         code[
    #             line] = "                     //'emis_map_%s_'//time_indice_str//'.csv', action='read')" % (
    #                 year)
    #     if code[line].startswith("                     //'emis_length_"):
    #         code[
    #             line] = "                     //'emis_length_%s_'//time_indice_str//'.csv', action='read')" % (
    #                 year)

    # # and write everything back
    # with open('%s/src/le_emis_dust_wind.F90' % (project_dir), 'w') as file:
    #     file.writelines(code)


def LOTOS_EUROS_dust_emis(project_dir: str,
                          iteration_num: int,
                          i_ensemble: int,
                          year=None) -> None:

    ### *--- edit the dust emission file ---* ###

    with open('%s/src/le_emis_dust_wind.F90' % (project_dir), 'r') as file:
        code = file.readlines()

    ### *--- input the pertubated dust emission field ---* ###
    for line in np.arange(1000, 1100):  # approximated range

        if code[line][0:23] == "       open(unit=3,file":

            code[
                line] = "       open(unit=3,file='/home/pangmj/Data/pyFilter/emission_map/emis_iteration/emis_iter_%02d/emis_ensem_%02d/'&\n" % (
                    iteration_num, i_ensemble)

        if not year is None:
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


### *--- add the ensemble meteo file into the model ---* ###
def LOTOS_EUROS_meteo_config(base_dir: str, meteo_num: int) -> None:

    ### *--- edit the lotos-euros-meteo-ecmwf.rc file ---* ####
    # open the lotos-euros-meteo-ecmwf.rc
    with open('%s/rc/lotos-euros-meteo-ecmwf.rc' % (base_dir), 'r') as file:
        code = file.readlines()

    for i_line in range(len(code)):
        if code[i_line].startswith('my.mf.dir'):
            code[
                i_line] = 'my.mf.dir           :  ${my.leip.dir}/ECMWF/od/ensm%02d/0001\n' % (
                    meteo_num)

    # and write everything back
    with open('%s/rc/lotos-euros-meteo-ecmwf.rc' % (base_dir), 'w') as file:
        file.writelines(code)


# ### *--- add the ensemble meteo file into the model ---* ###
# def LOTOS_EUROS_meteo_config(base_dir: str, meteo_num: int) -> None:

#     config_path = os.path.join(base_dir, 'rc', 'lotos-euros-meteo-ecmwf.rc')
#     with open(config_path, 'r') as file:
#         for i_line, line in enumerate(file):
#             if line.startswith('my.mf.dir'):
#                 new_line = 'my.mf.dir           :  ${my.leip.dir}/ECMWF/od/ensm%02d/0001\n'%(meteo_num:02d)
#                 with open(config_path, 'w') as output_file:
#                     output_file.writelines(
#                         line if not line.startswith('my.mf.dir') else new_line
#                         for line in file)
#                 break
