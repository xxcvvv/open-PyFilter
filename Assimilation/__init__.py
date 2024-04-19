'''
Autor: Mijie Pang
Date: 2024-03-23 14:49:47
LastEditTime: 2024-03-28 15:29:44
Description: 
'''
import os
import sys
import logging
import importlib
import subprocess
import multiprocessing as mp

main_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(main_dir)
import system_lib as stl
from tool.node import NodeScript, CheckNode


def entrance(Config: dict, **kwargs):

    home_dir = os.getcwd()
    status_path = os.path.join(main_dir, 'Status.json')

    ### tell the main branch that i am started ###
    stl.edit_json(path=status_path, new_dict={'assimilation': {'code': 10}})

    assimilation_scheme = Config['Assimilation']['scheme']['name']
    node_id = Config['Assimilation']['node']['node_id']
    core_demand = Config['Assimilation']['node']['core_demand']

    ### *--------------------------* ###
    ### *---      Gate Way      ---* ###
    ### run the script on gate ###
    if Config['Assimilation']['scheme']['run'] == 'gate':

        logging.info(
            'Assimilation project -> "%s" <- launched to gate' %
            (Config['Assimilation'][assimilation_scheme]['project_name']))

        ### run the script directly on the gate ###
        module = importlib.import_module('Assimilation.Assimilation_entrance')
        process = mp.Process(target=module.run,
                             name='Assimilation entrance',
                             args=(Config, ),
                             kwargs=kwargs)
        process.start()
        process.join()

        exit_code = process.exitcode
        if exit_code != 0:
            logging.error('Something is wrong!')
            sys.exit(exit_code)

    ### *--------------------------* ###
    ### *---      Node Way      ---* ###
    ### submit the script to the node ###
    elif Config['Assimilation']['scheme']['run'] == 'node':

        ### prepare the job submission script ###
        if Config['Assimilation']['node']['auto_node_selection']:
            check = CheckNode(Config['Info']['Machine']['management'])
            node_id = check.query(demand=core_demand,
                                  return_type='str',
                                  **Config['Assimilation']['node'])

        submission = NodeScript(
            path='%s/run_assimilation.sh' % (home_dir),
            node_id=node_id,
            core_demand=core_demand,
            job_name=Config['Assimilation'][assimilation_scheme]
            ['project_name'],
            management=Config['Info']['Machine']['management'])
        submission.add(
            'cd %s' % (home_dir), '%s -u Assimilation_entrance.py' %
            (Config['Info']['path']['my_python']))
        submit_command = submission.get_command()

        ### submit to the node ###
        command = subprocess.Popen(submit_command.split(' '),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)

        for info in iter(command.stdout.readline, b''):
            info_str = bytes.decode(info).strip()
            logging.debug(info_str)

        command.wait()

        ### record the status ###
        logging.info(
            'Assimilation project -> "%s" <- submitted to %s' %
            (Config['Assimilation'][assimilation_scheme]['project_name'],
             node_id))


if __name__ == '__main__':

    Config = stl.read_json(os.path.join(main_dir, 'config'), get_all=True)
    entrance(Config)
