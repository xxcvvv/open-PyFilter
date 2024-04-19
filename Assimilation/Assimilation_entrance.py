'''
Autor: Mijie Pang
Date: 2023-02-16 16:34:46
LastEditTime: 2024-04-05 19:30:25
Description: 
'''
import os
import sys
import logging
import importlib
import multiprocessing as mp

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
import system_lib as stl


def run(Config: dict, **kwargs):

    ### *---------------------------------------* ###
    ### *---   run the assimilation script   ---* ###

    model_scheme = Config['Model']['scheme']['name']
    assimilation_scheme = Config['Assimilation']['scheme']['name']
    module_name = Config['Script'][model_scheme]['Assimilation'][
        assimilation_scheme]
    logging.info('Module name : %s' % module_name)

    module = importlib.import_module('Assimilation.%s' % (module_name))

    process = mp.Process(target=module.main, args=(Config, ), kwargs=kwargs)
    process.start()
    process.join()

    ### *--- end of the process ---* ###
    exit_code = process.exitcode
    if exit_code != 0:
        logging.error('Something is wrong!')
        stl.edit_json(path=os.path.join(main_dir, 'Status.json'),
                      new_dict={'assimilation': {
                          'code': -1
                      }})
        sys.exit(exit_code)


if __name__ == '__main__':

    stl.Logging(log_level='DEBUG')
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    run(Config)
