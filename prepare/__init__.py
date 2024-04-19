'''
Autor: Mijie Pang
Date: 2024-03-23 14:40:48
LastEditTime: 2024-04-17 19:58:41
Description: 
'''
import os
import sys
import logging
import importlib
import multiprocessing as mp

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)


### *-----------------------------* ###
### *---   Start preparation   ---* ###
def entrance(Config: dict, Status: dict, **kwargs):

    if Config['Prepare']['jobs'] == []:
        logging.warning('nothing to do')

    ### *--- download the data needed ---* ###
    if 'obs_download' in Config['Prepare']['jobs']:

        module = importlib.import_module('prepare.obs_download')
        process = mp.Process(target=module.run,
                             args=(
                                 Config,
                                 Status,
                             ),
                             kwargs=kwargs)
        process.start()
        process.join()

        exit_code = process.exitcode
        if exit_code != 0:
            logging.error('Something is wrong!')
            sys.exit(exit_code)

    ### *--- download the input data ---* ###
    if 'input_download' in Config['Prepare']['jobs']:

        module = importlib.import_module('prepare.input_download')
        process = mp.Process(target=module.run,
                             args=(
                                 Config,
                                 Status,
                             ),
                             kwargs=kwargs)
        process.start()
        process.join()

        exit_code = process.exitcode
        if exit_code != 0:
            logging.error('Something is wrong!')
            sys.exit(exit_code)


if __name__ == '__main__':

    import system_lib as stl
    stl.Logging()
    Config = stl.read_json(os.path.join(main_dir, 'config'), get_all=True)
    Status = stl.read_json(os.path.join(main_dir, 'Status.json'))
    entrance(Config, Status)
