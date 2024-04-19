'''
Autor: Mijie Pang
Date: 2024-03-23 17:37:00
LastEditTime: 2024-04-06 16:07:26
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
from tool.node import NodeScript, CheckNode


def entrance(Config: dict, Status: dict, queue=None, **kwargs):

    check_node = CheckNode(Config['Info']['Machine']['management'])
    home_dir = os.getcwd()

    model_scheme = Config['Model']['scheme']['name']

    ### update the status code ###
    # stl.edit_json(path=status_path, new_dict={'model': {'post_process_code': 10}})

    ### *-------------------------------------* ###
    ### *---    run the post processing    ---* ###

    ### choice 1: do nothing to the model output ###
    if Config['Model'][model_scheme]['post_process']['save_method'] in [
            0, '0', 'none'
    ]:

        pass

    ### choice 2: save the original model output to somewhere else ###
    elif Config['Model'][model_scheme]['post_process']['save_method'] in [
            1, '1', 'origin', 2, '2', 'merge'
    ]:

        module_name = Config['Script'][model_scheme]['Model']['post_process'][
            'save']['origin']
        logging.debug('Module name : %s' % module_name)

        module = importlib.import_module('post_model.%s' % module_name)
        process = mp.Process(target=module.main, args=(
            Config,
            Status,
        ))
        process.start()
        process.join()

        exit_code = process.exitcode
        if exit_code != 0:
            logging.error('Something is wrong!')
            sys.exit(exit_code)

    ### after finish the necesssary procedure, the system continus ###
    queue.put('Please Go On')
    # stl.edit_json(path=status_path, new_dict={'model': {'post_process_code': 100}})

    ##################################################################
    ### parts that won't jam the system

    ### generate the combined product ###
    if Config['Model'][model_scheme]['post_process']['save_method'] in [
            2, '2', 'merge'
    ]:

        module_name = Config['Script'][model_scheme]['Model']['post_process'][
            'save']['merge']
        logging.debug('Module name : %s' % module_name)

        module = importlib.import_module('post_model.%s' % module_name)
        process = mp.Process(target=module.main, args=(
            Config,
            Status,
        ))
        process.start()
        process.join()

        exit_code = process.exitcode
        if exit_code != 0:
            logging.warning('Something is wrong!')
            sys.exit(exit_code)

    ### plot the model forecast results ###
    if Config['Model'][model_scheme]['post_process'][
            'plot_results'] and not Config['Script'][model_scheme]['Model'][
                'post_process']['plot'] == '':

        run_spec = Config['Model'][model_scheme]['post_process']['run_spec']

        if run_spec[0] == 'gate':

            module_name = Config['Script'][model_scheme]['Model'][
                'post_process']['plot']
            logging.debug('Module name : %s' % module_name)

            module = importlib.import_module('post_model.%s' % module_name)
            process = mp.Process(target=module.main, args=(
                Config,
                Status,
            ))
            process.start()
            process.join()

            exit_code = process.exitcode
            if exit_code != 0:
                logging.warning('Something is wrong!')
                sys.exit(exit_code)

        elif run_spec[0] == 'node':

            node_id = check_node.query(demand=run_spec[1], return_type='str')

            submission = NodeScript(
                path='%s/qsub_post_process.sh' % (home_dir),
                node_id=node_id,
                core_demand=run_spec[1],
                job_name='post_forecast',
                management=Config['Info']['Machine']['management'])
            submission.add(
                'cd %s' % (home_dir), '%s %s' %
                (Config['Info']['path']['my_python'], Config['Script']
                 [model_scheme]['Model']['post_process']['plot']))
            submit_command = submission.get_command()

            command = os.system(submit_command)

    ### end of part
    ##################################################################


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json(os.path.join(main_dir, 'config'), get_all=True)
    Status = stl.read_json(path=os.path.join(main_dir, 'Status.json'))
    entrance(Config, Status)
