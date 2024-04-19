'''
Autor: Mijie Pang
Date: 2023-04-24 19:36:09
LastEditTime: 2024-04-04 17:42:51
Description: guiding to the selected model and ensemble running method
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
    module_name = Config['Script'][model_scheme]['Model'][
        Config['Model'][model_scheme]['run_type']]
    logging.info('Module name : %s' % module_name)

    module = importlib.import_module('Model.%s' % module_name)

    process = mp.Process(target=module.main, args=(Config, ), kwargs=kwargs)
    process.start()
    process.join()

    ### *--- end of the process ---* ###
    exit_code = process.exitcode
    if exit_code != 0:
        logging.error('Something is wrong!')
        stl.edit_json(path=os.path.join(main_dir, 'Status.json'),
                      new_dict={'model': {
                          'code': -1
                      }})
        sys.exit(exit_code)


if __name__ == '__main__':
    
    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    run(Config)
