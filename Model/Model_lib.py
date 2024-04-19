'''
Autor: Mijie Pang
Date: 2023-04-22 16:32:34
LastEditTime: 2024-03-05 08:50:14
Description: This is the library for the universal model operation
'''
import os


### *--- create sub model project ---* ###
def copy_from_source(model_dir: str, model_dir_sub: str, run_id: str) -> str:

    sub_dir = model_dir_sub + '/' + run_id

    if not os.path.exists(sub_dir):
        os.makedirs(sub_dir)

    command = os.system('rm -rf ' + sub_dir + '/*')
    command = os.system('rsync -a ' + model_dir + '/* ' + sub_dir)

    return sub_dir
