'''
Autor: Mijie Pang
Date: 2023-04-22 16:32:34
LastEditTime: 2023-09-02 15:28:32
Description: This is the library for the universal model operation
'''
import os
import sys
import time
import logging

sys.path.append('../')
from system_lib import get_available_node


### create sub project ###
def copy_from_source(model_dir: str, model_dir_sub: str, run_id: str) -> str:

    sub_dir = model_dir_sub + '/' + run_id

    if not os.path.exists(sub_dir):
        os.makedirs(sub_dir)

    command = os.system('rm -rf ' + sub_dir + '/*')
    command = os.system('rsync -a ' + model_dir + '/* ' + sub_dir)

    return sub_dir


### arange all the ensemble to a node list ###
def arange_node_list(available_num: list, ensemble_number: int,
                     core_demand: int) -> list:

    core_demand = int(core_demand)
    ensemble_number = int(ensemble_number)

    ### check if the available node is sufficient ###
    available_flag = False
    while not available_flag:

        total_available = sum(
            [available_num[i_node][1] for i_node in range(len(available_num))])
        if ensemble_number * core_demand <= total_available:
            available_flag = True
        else:
            logging.warning('node busy')
            time.sleep(60)
            available_num = get_available_node(demand=core_demand,
                                               reserve=0,
                                               return_type='number_list')

    ### arrange a node list ###
    node_list = []
    for i in range(ensemble_number):
        for j in range(len(available_num)):

            if available_num[j][1] - core_demand >= 0:
                # print(available_num)
                node_list.append(int(available_num[j][0]))
                available_num[j][1] = available_num[j][1] - core_demand
                break

    return node_list
