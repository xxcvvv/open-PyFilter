'''
Autor: Mijie Pang
Date: 2023-09-16 16:46:40
LastEditTime: 2024-03-23 09:28:46
Description: designed for jobs running on node
'''
import os
import time
import random
import logging

from decorators import deprecated


### prepare the script to submit the job to the node ###
@deprecated
class NodeScript:

    def __init__(self,
                 path='default.sh',
                 node_id=1,
                 core_demand=1,
                 job_name='test',
                 out_file='',
                 error_file='',
                 parallel_mode='smp',
                 executor='/bin/bash',
                 management='SGE') -> None:

        script_list = []

        ### Strategy for Sun Grid Engine (SGE) ###
        if management == 'SGE':

            ### generate the submission script worked on SGE ###
            script_list.append('#!%s' % (executor))  # assign the shell
            script_list.append('#$ -V')  #  current environment Variables
            script_list.append('#$ -cwd')  # current working directory
            script_list.append('#$ -S %s' % (executor))  # shell environment
            script_list.append(
                '#$ -j y')  #standard error to the output file (y|n)

            # assign the submission node id
            if isinstance(node_id, int) or node_id.isdigit():
                script_list.append('#$ -q all.q@compute-0-%s.local' %
                                   (node_id))
            elif isinstance(node_id, str):
                script_list.append('#$ -q %s' % (node_id))

            # assign the standard output file
            if out_file == '':
                out_file = job_name + '_out.log'
            script_list.append('#$ -o %s' % (out_file))

            # assign the standard error file
            if error_file == '':
                error_file = job_name + '_error.log'
            script_list.append('#$ -e %s' % (error_file))

            script_list.append('#$ -N %s' % (job_name))  #  job name
            # assign the parallel computing mode and number
            script_list.append('#$ -pe %s %s' % (parallel_mode, core_demand))

        ### Strategy for Simple Linux Utility for Resource Management (SLURM) ###
        elif management == 'SLURM':

            ### generate the submission script worked on SLURM ###
            script_list.append('#!' + executor)
            script_list.append('#SBATCH -n ')
            script_list.append('#SBATCH -N ')
            script_list.append('#SBATCH -t ')
            script_list.append('#SBATCH -p ')
            script_list.append('#SBATCH --mem=')
            script_list.append('#SBATCH -o ')
            script_list.append('#SBATCH -e ')

        ### Strategy for Portable Batch System (PBS) ###
        elif management == 'PBS':

            ### generate the submission script worked on SLURM ###
            script_list.append('#!' + executor)  # executor
            script_list.append('#PBS -S %s' % (executor))  #shell environment
            script_list.append('#PBS -V')  # current environment Variables
            script_list.append('#PBS -N %s' % (job_name))  #  job name
            script_list.append('#PBS -l nodes=%s:ppn=%s' %
                               (core_demand, node_id))  # node number and cores

            # assign the standard output file
            if out_file == '':
                out_file = job_name + '_out.log'
            script_list.append('#PBS -o %s' % (out_file))

            # assign the standard error file
            if error_file == '':
                error_file = job_name + '_error.log'
            script_list.append('#PBS -e %s' % (error_file))

        else:

            return ValueError('Management : %s is not supported yet' %
                              (management))

        self.path = path
        self.management = management
        self.script_list = script_list

    ### add extra config scripts if necessary ###
    def add_config(self, **kwargs) -> None:

        if self.management == 'SGE':
            prefix = '#$'
        elif self.management == 'SLURM':
            prefix = '#SBATCH'
        elif self.management == 'PBS':
            prefix = '#PBS'

        for key in kwargs.keys():
            self.script_list.append('%s %s %s' % (prefix, key, kwargs[key]))

    ### add run scripts ###
    def add(self, *args) -> None:

        self.script_list += args

    ### write to the submission file ###
    def write(self, ) -> None:

        with open(self.path, 'w') as file:
            file.write('\n'.join(self.script_list))

    ### get the submission command ###
    def get_command(self) -> str:

        with open(self.path, 'w') as file:
            file.write('\n'.join(self.script_list))

        ### return the submission command ###
        script_name = self.path.split('/')[-1]
        if self.management == 'SGE' or self.management == 'PBS':
            submit_command = 'qsub ' + script_name
        elif self.management == 'SLURM':
            submit_command = 'sbatch ' + script_name
        return submit_command


### prepare the script to submit the job to the node ###
class NodeScript_test:

    def __init__(self,
                 path='default.sh',
                 node_id=1,
                 core_demand=1,
                 job_name='test',
                 out_file='',
                 error_file='',
                 parallel_mode='smp',
                 executor='/bin/bash',
                 management='SGE',
                 **kwargs) -> None:

        self.path = path
        self.node_id = node_id
        self.core_demand = core_demand
        self.executor = executor
        self.job_name = job_name
        self.parallel_mode = parallel_mode
        self.management = management
        self.out_file = out_file if out_file else f'{job_name}_out.log'
        self.error_file = error_file if error_file else f'{job_name}_error.log'

        self.script_list = self._generate_script(**kwargs)

    ### *-----------------------------------------------* ###
    ### *---   Generate the node submission script   ---* ###
    ### *-----------------------------------------------* ###
    def _generate_script(self, **kwargs):

        manager_strategy = {
            'SGE': self._generate_sge,
            'SLURM': self._generate_slurm,
            'PBS': self._generate_pbs
        }

        if self.management in manager_strategy:
            return manager_strategy[self.management](**kwargs)
        else:
            raise ValueError(
                f'Management : {self.management} is not supported yet')

    ### *--- Strategy for Sun Grid Engine (SGE) ---* ###
    def _generate_sge(self, **kwargs) -> list:

        executor = kwargs.get('executor', self.executor)
        node_id = kwargs.get('node_id', self.node_id)
        job_name = kwargs.get('job_name', self.job_name)
        parallel_mode = kwargs.get('parallel_mode', self.parallel_mode)
        core_demand = kwargs.get('core_demand', self.core_demand)
        out_file = kwargs.get('out_file', self.out_file)
        error_file = kwargs.get('error_file', self.error_file)

        script_list = []

        ### generate the submission script worked on SGE ###
        script_list.append('#!%s' % (executor))  # assign the shell
        script_list.append('#$ -V')  #  current environment Variables
        script_list.append('#$ -cwd')  # current working directory
        script_list.append('#$ -S %s' % (executor))  # shell environment
        script_list.append('#$ -j y')  #standard error to the output file (y|n)

        # assign the submission node id
        if isinstance(node_id, int) or node_id.isdigit():
            script_list.append('#$ -q all.q@compute-0-%s.local' % (node_id))
        elif isinstance(node_id, str):
            script_list.append('#$ -q %s' % (node_id))

        # assign the standard output file
        script_list.append('#$ -o %s' % (out_file))

        # assign the standard error file
        script_list.append('#$ -e %s' % (error_file))

        script_list.append('#$ -N %s' % (job_name))  #  job name
        # assign the parallel computing mode and number
        script_list.append('#$ -pe %s %s' % (parallel_mode, core_demand))

        return script_list

    ### *--- Strategy for Simple Linux Utility for Resource Management (SLURM) ---* ###
    def _generate_slurm(self, **kwargs) -> list:

        executor = kwargs.get('executor', self.executor)
        node_id = kwargs.get('node_id', self.node_id)
        job_name = kwargs.get('job_name', self.job_name)
        parallel_mode = kwargs.get('parallel_mode', self.parallel_mode)
        core_demand = kwargs.get('core_demand', self.core_demand)
        out_file = kwargs.get('out_file', self.out_file)
        error_file = kwargs.get('error_file', self.error_file)

        script_list = []

        ### generate the submission script worked on SLURM ###
        script_list.append('#!' + executor)
        script_list.append('#SBATCH -n ')
        script_list.append('#SBATCH -N ')
        script_list.append('#SBATCH -t ')
        script_list.append('#SBATCH -p ')
        script_list.append('#SBATCH --mem=')
        script_list.append('#SBATCH -o ')
        script_list.append('#SBATCH -e ')

        return script_list

    ### *--- Strategy for Portable Batch System (PBS) ---* ###
    def _generate_pbs(self, **kwargs) -> list:

        executor = kwargs.get('executor', self.executor)
        node_id = kwargs.get('node_id', self.node_id)
        job_name = kwargs.get('job_name', self.job_name)
        parallel_mode = kwargs.get('parallel_mode', self.parallel_mode)
        core_demand = kwargs.get('core_demand', self.core_demand)
        out_file = kwargs.get('out_file', self.out_file)
        error_file = kwargs.get('error_file', self.error_file)

        script_list = []

        ### generate the submission script worked on SLURM ###
        script_list.append('#!' + executor)  # executor
        script_list.append('#PBS -S %s' % (executor))  #shell environment
        script_list.append('#PBS -V')  # current environment Variables
        script_list.append('#PBS -N %s' % (job_name))  #  job name
        script_list.append('#PBS -l nodes=%s:ppn=%s' %
                           (core_demand, node_id))  # node number and cores

        # assign the standard output file
        if out_file == '':
            out_file = job_name + '_out.log'
        script_list.append('#PBS -o %s' % (out_file))

        # assign the standard error file
        if error_file == '':
            error_file = job_name + '_error.log'
        script_list.append('#PBS -e %s' % (error_file))

        return script_list

    ### *--- add extra config scripts if necessary ---* ###
    def add_config(self, **kwargs) -> None:

        prefix_options = {'SGE': '#$', 'SLURM': '#SBATCH', 'PBS': '#PBS'}
        prefix = prefix_options.get(self.management, '')
        for key, value in kwargs.items():
            self.script_list.append(f"{prefix} {key} {value}")

    ### *--- add run scripts ---* ###
    def add(self, *args) -> None:

        self.script_list += args

    ### *--- write to the submission file ---* ###
    def write(self, ) -> None:

        with open(self.path, 'w') as file:
            file.write('\n'.join(self.script_list))

    ### *--- get the submission command ---* ###
    def get_command(self) -> str:

        # Ensure the script is written before returning the command
        self.write()

        # return the submission command
        script_name = os.path.basename(self.path)
        command_options = {'SGE': 'qsub', 'SLURM': 'sbatch', 'PBS': 'qsub'}
        submit_command = command_options.get(
            self.management) + ' ' + script_name
        return submit_command


### *--- check the available cores in node ---* ###
class CheckNode:

    def __init__(self, management='SGE') -> None:

        managements = {
            'SGE': self.SGE_query,
            'PBS': self.PBS_query,
            'SLURM': self.SLURM_query
        }
        if management not in managements.keys():
            raise ValueError('Management : "%s" is not supported yet' %
                             (self.management))
        else:
            self.management = management
            self.managements = managements

    ### *------------------------* ###
    ### *---   Query Portal   ---* ###
    def query(self, **kwargs) -> None:

        return self.managements.get(self.management)(**kwargs)

    ### *-- querry the Sun Grid Engine node ---* ###
    def SGE_query(self,
                  demand=1,
                  random_choice=False,
                  return_type='number',
                  reserve=0,
                  wait_time=120,
                  black_list=['1', '2', '3', '12'],
                  load_max=50,
                  mem_min=10,
                  **kwargs) -> None:

        ### *--- settings ---* ###
        demand = kwargs.get('demand', demand)
        random_choice = kwargs.get('random_choice', random_choice)
        return_type = kwargs.get('return_type', return_type)
        reserve = kwargs.get('reserve', reserve)
        wait_time = kwargs.get('wait_time', wait_time)  # minutes
        black_list = kwargs.get('black_list', black_list)
        load_max = kwargs.get(
            'load_max', load_max)  # set the threshold of load on cpu usage
        mem_min = kwargs.get('mem_min',
                             mem_min)  # the minimum memory needed, unit : GB

        ### *--- Main Loop ---* ###
        for wait in range(wait_time):

            ### *--- check the node information ---* ###
            process = os.popen('qstat -f')  # return file
            # columns : queuename qtype resv/used/tot. load_avg arch states
            node_querry1 = process.read().split('\n')
            process.close()

            process = os.popen('qhost')  # return file
            # columns : HOSTNAME ARCH NCPU NSOC NCOR NTHR LOAD MEMTOT MEMUSE SWAPTO SWAPUS
            node_querry2 = process.read().split('\n')
            process.close()

            ### *--- arrange the information ---* ###
            node_info = {}
            for line in node_querry1:
                if line.startswith('all.q@compute-'):

                    info_split = line.split()
                    node_info[info_split[0]] = {
                        'number': info_split[0].split('-')[2].split('.')[0],
                        'core_total': int(info_split[2].split('/')[2]),
                        'core_used': int(info_split[2].split('/')[1]),
                        'load_avg': float(info_split[3])
                    }
            for line in node_querry2:
                if line.startswith('compute-'):
                    info_split = line.split()
                    node_info['all.q@%s.local' % (info_split[0])].update({
                        'mem_total':
                        float(info_split[7][:-1]),
                        'mem_used':
                        float(info_split[8][:-1])
                    })

            ### *--- screen the unqualified node ---* ###
            for node in node_info.copy().keys():

                ### exclude the node in black list ###
                if not node_info[node]['number'] in black_list:

                    ### decide if the node has enough core I need ###
                    condition1 = reserve <= node_info[node][
                        'core_total'] - node_info[node]['core_used'] - demand

                    ### decide if the node are too busy ###
                    condition2 = node_info[node]['load_avg'] < load_max

                    ### decide if the node has enough memory I need ###
                    condition3 = node_info[node]['mem_total'] - node_info[
                        node]['mem_used'] > mem_min

                    if not condition1 or not condition2 or not condition3:
                        node_info.pop(node)

                else:
                    node_info.pop(node)

            if len(node_info) == 0:
                logging.warning('node busy')
                time.sleep(60)
            else:
                # logging.debug(node_info)
                break

        ### *--- produce the output ---* ###
        # available_num.append([node_id, total - used - reserve])
        # available_str.append([node_info[0], total - used - reserve])

        ### if there isn't enough available core detected in due time,
        ### return the false flag
        if len(node_info) == 0:

            raise TimeoutError('No available core in due time (%s mins)' %
                               (wait_time))

        ### *--- decide which type of the result to return ---* ###
        # return a node number
        if return_type == 'number':

            if random_choice:
                node_id = random.choice(list(node_info.keys()))
            else:
                node_id = sorted(list(node_info.keys()))[0]

            return int(node_info[node_id]['number'])

        # return a node name string
        elif return_type == 'str':

            if random_choice:
                node_id = random.choice(list(node_info.keys()))
            else:
                node_id = sorted(list(node_info.keys()))[0]

            return node_id

        # return a list contains all the available node number
        elif return_type == 'number_list':

            number_list = []
            for node_id in node_info.keys():
                number_list.append([
                    node_info[node_id]['number'],
                    node_info[node_id]['core_total'] -
                    node_info[node_id]['core_used'] - reserve
                ])
            return number_list

        # return a list contains all the available node string
        elif return_type == 'str_list':

            str_list = []
            for node_id in node_info.keys():
                str_list.append([
                    node_id, node_info[node_id]['core_total'] -
                    node_info[node_id]['core_used'] - reserve
                ])
            return str_list

    def PBS_query(self, ):
        pass

    def SLURM_query(self, ):
        pass


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
            available_num = CheckNode.query(demand=core_demand,
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


if __name__ == '__main__':

    # check = CheckNode(management='kgs')
    # print(check.query(return_type='str_list'))

    submit = NodeScript_test()
    submit.add_config(a=8932)
    submit.add('falskdjfd', 'fdoajfds', 'fodajifdsa', 'fsdaf', 'dfaoisdf',
               'oivjcbx')
    submit.write()
    print(submit.get_command())

    # test = CheckNode('SGE')
    # print(test.query())
