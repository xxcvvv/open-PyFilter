'''
Autor: Mijie Pang
Date: 2024-03-23 14:57:19
LastEditTime: 2024-03-24 09:38:12
Description: 
'''
import os
import sys
import logging
import importlib
import multiprocessing as mp

main_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(main_dir)
import system_lib as stl


def entrance(Config: dict, **kwargs):

    ### *--- run the model ---* ###
    module = importlib.import_module('Model.Model_entrance')
    process = mp.Process(target=module.run, args=(Config, ), kwargs=kwargs)
    process.start()
    process.join()

    exit_code = process.exitcode
    if exit_code != 0:
        logging.error('Something is wrong!')
        sys.exit(exit_code)


if __name__ == '__main__':

    Config = stl.read_json(os.path.join(main_dir, 'config'), get_all=True)
    entrance(Config)
