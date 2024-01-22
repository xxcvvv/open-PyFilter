'''
Autor: Mijie Pang
Date: 2023-04-22 19:53:41
LastEditTime: 2024-01-04 20:25:44
Description: 
'''
import re
import os
import time
import json
import random
import inspect
import logging
import pandas as pd
from glob import iglob
from datetime import datetime, timedelta


def read_json(path: str, mode='r') -> dict:

    with open(path, mode) as f:
        load_dict = json.load(f)

    return load_dict


def read_json_dict(path: str, get_all=False, *args) -> dict:

    load_dict = {}
    ### read all the json files ###
    if get_all:

        json_files = iglob(os.path.join(path, '*.json'))
        for json_file in json_files:

            with open(json_file, 'r') as f:
                load_dict.update(
                    {os.path.split(json_file)[-1].split('.')[0]: json.load(f)})

    ### only read given json files ###
    else:

        for arg in args:
            with open(os.path.join(path, arg), 'r') as f:
                load_dict.update({arg.split('.')[0]: json.load(f)})

    return load_dict


def update_dict(dict1: dict, dict2: dict) -> dict:

    for key, value in dict2.items():
        if key in dict1 and isinstance(value, dict) and isinstance(
                dict1[key], dict):
            update_dict(dict1[key], dict2[key])
        else:
            dict1[key] = value

    return dict1


def edit_json(path: str, new_dict: dict) -> None:

    with open(path, 'r') as jsonFile:
        data = json.load(jsonFile)

    data = update_dict(data, new_dict)

    with open(path, 'w') as jsonFile:
        json.dump(data, jsonFile)


### to count the process time of an function ###
def timer(count=True):

    def decorator(func):

        def wrapper(*args, **kwargs):

            start = datetime.now()
            result = func(*args, **kwargs)
            if count:
                print('Func %s took ：%.2f s' %
                      (func.__name__,
                       (datetime.now() - start).total_seconds()))

            return result

        return wrapper

    return decorator


### design for advanced log record ###
class Logging:

    def __init__(self, log_path: str, level='INFO') -> None:

        # log level : DEBUG -> INFO -> WARNING -> ERROR -> CRITICAL

        frame = inspect.currentframe().f_back
        filename = inspect.getframeinfo(frame).filename
        basename = os.path.basename(filename)

        if not log_path.endswith('.log'):
            log_path += '.log'

        logging.basicConfig(
            filename=log_path,
            format='[%(levelname)s] %(asctime)s -> ' + basename +
            ' : %(message)s',  # log format
            filemode='a',
            level=logging.getLevelName(level),  # log level 
            datefmt='%Y-%m-%d %H:%M:%S')

        self.logging = logging
        self.log_path = log_path
        self.level = level

    def refresh(self, ) -> None:

        logger = logging.getLogger()
        for handler in logger.handlers:
            logger.removeHandler(handler)

        frame = inspect.currentframe().f_back
        filename = inspect.getframeinfo(frame).filename
        name = os.path.basename(filename)

        self.logging.basicConfig(
            filename=self.log_path,
            format='[%(levelname)s] %(asctime)s -> ' + name +
            ' : %(message)s',  # log format
            filemode='a',
            level=logging.getLevelName(self.level),  # log level 
            datefmt='%Y-%m-%d %H:%M:%S')

        self.logging = logging

    def debug(self, *words):
        for word in words:
            self.logging.debug(word)

    def info(self, *words):
        for word in words:
            self.logging.info(word)

    def warning(self, *words):
        for word in words:
            self.logging.warning(word)

    def error(self, *words):
        for word in words:
            self.logging.error(word)

    def critical(self, *words):
        for word in words:
            self.logging.critical(word)

    def write(self, *words):

        with open(self.log_path, 'a') as log_file:
            for word in words:
                log_file.write(word + '\n')


### mark the number of the assimilation loop ###
def number_guide(number: int) -> str:

    line='\n######--------------------######\n'+\
         '######      Round {:0>4d}    ######\n'.format(number)+\
         '######--------------------######'

    return line


### get the list of all assimilation moments ###
def get_run_time(config: dict) -> list:

    scheme = config['scheme']

    if scheme == 'equal_step':

        asml_time_list = pd.date_range(
            datetime.strptime(config[scheme]['first_run_time'],
                              '%Y-%m-%d %H:%M:%S'),
            datetime.strptime(config[scheme]['last_run_time'],
                              '%Y-%m-%d %H:%M:%S'),
            freq=config[scheme]['asml_time_interval'])

        asml_time_list = asml_time_list.strftime(
            '%Y-%m-%d %H:%M:%S').tolist()  # type : str

        model_time_list = get_end_time(asml_time_list,
                                       config[scheme]['run_time_range'])

    elif scheme == 'assign':
        pass

    else:
        raise ValueError('run time generation method : %s is not supported' %
                         (scheme))

    return asml_time_list, model_time_list


### calculate the end time of model run ###
def get_end_time(start_time: list, duration_str: str) -> list:

    ### parse the duration string ###
    pattern = r'(\d+)([A-Za-z]+)'
    durations = re.findall(pattern, duration_str)

    delta = timedelta()
    for amount, unit in durations:
        if unit == 'D':
            delta += timedelta(days=int(amount))
        elif unit == 'H':
            delta += timedelta(hours=int(amount))
        elif unit == 'M':
            delta += timedelta(minutes=int(amount))
        elif unit == 'S':
            delta += timedelta(seconds=int(amount))

    if delta.total_seconds() == 0:

        return []

    else:

        end_time = []
        for i_time in range(len(start_time)):
            start_time_temp = datetime.strptime(start_time[i_time],
                                                '%Y-%m-%d %H:%M:%S')
            end_time.append(
                (start_time_temp + delta).strftime('%Y-%m-%d %H:%M:%S'))

        time_list = [[a, b] for a, b in zip(start_time, end_time)]

        return time_list


### split the list into few parts ###
def split_list(lst: list, num_parts: int) -> list:

    avg = len(lst) / float(num_parts)
    result = []
    last = 0.0

    while last < len(lst):
        result.append(lst[int(last):int(last + avg)])
        last += avg

    return result


### check and wait until the part finishes ###
def check_for_status(path: str,
                     section: str,
                     key: str,
                     interval=3,
                     check_start=True) -> str:

    if check_start:

        ### make sure it starts ###
        wait_start_flag = True
        while wait_start_flag:

            with open(path, 'r') as f:
                status = json.load(f)[section][key]

            if status == 'T' or status == 'E':
                wait_start_flag = False

            elif status == 'F':
                time.sleep(interval)

    ### check if it is finished ###
    wait_finish_flag = True
    while wait_finish_flag:

        with open(path, 'r') as f:
            status = json.load(f)[section][key]

        if status == 'F' or status == 'E':
            wait_finish_flag = False

        elif status == 'T':
            time.sleep(interval)

    return status


### check and wait until the part finishes ###
def check_status_code(path: str,
                      section: str,
                      key: str,
                      interval=3,
                      check_start=True) -> int:

    if check_start:

        ### make sure it starts ###
        wait_start_flag = True
        while wait_start_flag:

            with open(path, 'r') as f:
                status = json.load(f)[section][key]

            if not status == 0:
                wait_start_flag = False
            else:
                time.sleep(interval)

    ### check if it is finished ###
    wait_finish_flag = True
    while wait_finish_flag:

        with open(path, 'r') as f:
            status = json.load(f)[section][key]

        if status == 100 or status < 0:
            wait_finish_flag = False
        else:
            time.sleep(interval)

    return status


### check the queue and get available node id ###
def get_available_node(
        demand=1,
        black_list=['3', '4', '5', '12', '2', '10'],
        return_type='number',
        reserve=0,
        wait_time=120,  # mins
        random_choice=False,
        management='SGE'):

    if management == 'SGE':

        for wait in range(wait_time):

            available_num = []
            available_str = []

            process = os.popen('qstat -f')  # return file
            lines = process.read().split('\n')
            process.close()

            for line in lines:

                ### select the line contains the information about the node ###
                if line.startswith('all.q@compute-'):
                    node_id = (
                        line.split('.')[1]).split('-')[2]  # single node number

                    ### exclude the node in black list ###
                    if not node_id in black_list:

                        ### get the total and used core number of the node ###
                        total = int((line.split('/')[2]).split(' ')[0])
                        used = int(line.split('/')[1])

                        ### decide if the node has enough core I need ###
                        if reserve <= total - used - demand:
                            available_num.append(
                                [node_id, total - used - reserve])
                            available_str.append(
                                [line.split(' ')[0], total - used - reserve])

            if available_num == []:
                logging.warning('node busy')
                time.sleep(60)
            elif not available_num == []:
                break

        ### if there isn't enough available core detected in due time,
        # return the false flag ###
        if available_num == []:
            logging.error('NO available core in due time (%s mins)' %
                          (wait_time))
            return False

        ### decide which type of the result to return ###
        # return a node number
        if return_type == 'number':
            node_id = available_num[0][0]
            if random_choice:
                node_id = random.choice(available_num)[0]
            return int(node_id)

        # return a node name string
        elif return_type == 'str':
            node_id = available_str[0][0]
            if random_choice:
                node_id = random.choice(available_str)[0]
            return str(node_id)

        # return a list contains all the available node number
        elif return_type == 'number_list':
            return available_num

        # return a list contains all the available node string
        elif return_type == 'str_list':
            return available_str

    else:
        logging.error('Management : %s is not supported yet' % (management))
        return False


### generate from "http://patorjk.com/software/taag/#p=display&h=0&v=0&f=Slant&t=PyFilter" ###
def welcome() -> str:

    line =  ' _       __          __                                 \n' + \
            '| |     / /  ___    / /  _____  ____    ____ ___   ___  \n' + \
            '| | /| / /  / _ \  / /  / ___/ / __ \  / __ `__ \ / _ \ \n' + \
            '| |/ |/ /  /  __/ / /  / /__  / /_/ / / / / / / //  __/ \n' + \
            '|__/|__/   \___/ /_/   \___/  \____/ /_/ /_/ /_/ \___/  \n' + \
            '    ____              ______    _     __   __                \n' + \
            '   / __ \   __  __   / ____/   (_)   / /  / /_  ___    _____ \n' + \
            '  / /_/ /  / / / /  / /_      / /   / /  / __/ / _ \  / ___/ \n' + \
            ' / ____/  / /_/ /  / __/     / /   / /  / /_  /  __/ / /     \n' + \
            '/_/       \__, /  /_/       /_/   /_/   \__/  \___/ /_/      \n' + \
            '         /____/                                              \n'

    return line


def finished() -> str:

    line =  '    ______    _             _             __               __\n' + \
            '   / ____/   (_)   ____    (_)   _____   / /_   ___   ____/ /\n' + \
            '  / /_      / /   / __ \  / /   / ___/  / __ \ / _ \ / __  / \n' + \
            ' / __/     / /   / / / / / /   (__  )  / / / //  __// /_/ /  \n' + \
            '/_/       /_/   /_/ /_/ /_/   /____/  /_/ /_/ \___/ \__,_/   \n'

    return line
