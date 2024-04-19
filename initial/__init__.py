'''
Autor: Mijie Pang
Date: 2024-03-23 09:57:24
LastEditTime: 2024-03-24 09:34:56
Description: 
'''
import os
import sys
import logging
import importlib
import multiprocessing as mp


### *-------------------------------------* ###
### *---      Run Initialization      ----* ###
def entrance(Config: dict, **kwargs) -> None:

    if 'install_restart' in Config['Initial']['jobs']:

        model_scheme = Config['Model']['scheme']['name']
        module_name = Config['Script'][model_scheme]['initial'][
            'install_restart']
        logging.info('Module name : %s' % module_name)

        module = importlib.import_module('initial.%s' % module_name)

        process = mp.Process(target=module.main,
                             args=(Config, ),
                             kwargs=kwargs)
        process.start()
        process.join()

        exit_code = process.exitcode
        if exit_code != 0:
            logging.error('Something is wrong!')
            sys.exit(exit_code)

    if 'initial_run' in Config['Initial']['jobs']:

        model_scheme = Config['Model']['scheme']['name']
        module_name = Config['Script'][model_scheme]['initial']['initial_run']
        logging.info('Module name : %s' % module_name)

        module = importlib.import_module('initial.%s' % module_name)

        process = mp.Process(target=module.main,
                             args=(Config, ),
                             kwargs=kwargs)
        process.start()
        process.join()

        exit_code = process.exitcode
        if exit_code != 0:
            logging.error('Something is wrong!')
            sys.exit(exit_code)


if __name__ == '__main__':

    main_dir = os.path.dirname(os.path.dirname(__file__))
    sys.path.append(main_dir)
    import system_lib as stl

    Config = stl.read_json(os.path.join(main_dir, 'config'), get_all=True)

    entrance(Config)
