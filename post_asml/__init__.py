'''
Autor: Mijie Pang
Date: 2024-03-23 17:37:31
LastEditTime: 2024-04-05 15:35:22
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


def entrance(Config: dict, Status: dict, **kwargs):

    ### *--- plot the results ---* ###
    # if Config['Assimilation']['post_process']['plot_results']:

    #     model_scheme = Config['Model']['scheme']['name']
    #     module_name = Config['Script'][model_scheme]['Assimilation'][
    #         'post_process']['plot']
    #     logging.debug('Module name : %s' % module_name)

    #     module = importlib.import_module('post_asml.%s' % module_name)
    #     process = mp.Process(target=module.main,
    #                          args=(Config, Status,),
    #                          kwargs=kwargs)
    #     process.start()
    #     process.join()

    #     exit_code = process.exitcode
    #     if exit_code != 0:
    #         logging.warning('Something is wrong!')
    #         sys.exit(exit_code)

    logging.warning('Nothing to do')


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json(os.path.join(main_dir, 'config'), get_all=True)
    Status = stl.read_json(path=os.path.join(main_dir, 'Status.json'))
    entrance(Config, Status)
