'''
Autor: Mijie Pang
Date: 2023-04-22 19:53:41
LastEditTime: 2024-04-17 19:25:52
Description: 
'''
import re
import os
import time
import json
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

    if get_all:
        json_files = iglob(os.path.join(path, '*.json'))
    else:
        json_files = [os.path.join(path, arg) for arg in args]

    for json_file in json_files:
        with open(json_file, 'r') as f:
            key = os.path.splitext(os.path.basename(json_file))[0]
            load_dict[key] = json.load(f)

    return load_dict


def update_dict(dict1: dict, dict2: dict) -> dict:

    for key, value in dict2.items():
        if key in dict1 and isinstance(value, dict) and isinstance(
                dict1[key], dict):
            update_dict(dict1[key], dict2[key])
        else:
            dict1[key] = value

    return dict1


def edit_json(path: str, new_dict: dict) -> dict:

    with open(path, 'r') as jsonFile:
        data = json.load(jsonFile)

    data = update_dict(data, new_dict)

    with open(path, 'w') as jsonFile:
        json.dump(data, jsonFile)

    return data


### *--- design for advanced log record ---* ###
class Logging:

    def __init__(self,
                 home_dir='.',
                 log_name=None,
                 log_level='INFO',
                 **kwargs) -> None:

        self.home_dir = home_dir
        self.log_name = kwargs.get('log_name', log_name)
        self.log_level = kwargs.get('log_level', log_level)

        log_path = os.path.join(self.home_dir,
                                self.log_name) if self.log_name else None

        logging.getLogger('matplotlib').setLevel(logging.WARNING)
        logging.getLogger('fiona').setLevel(logging.WARNING)

        logging.basicConfig(filename=log_path,
                            format='[%(levelname)s] %(asctime)s : %(message)s',
                            filemode='a',
                            level=logging.getLevelName(self.log_level),
                            datefmt='%Y-%m-%d %H:%M:%S')

    def debug(self, *words):
        for word in words:
            logging.debug(word)

    def info(self, *words):
        for word in words:
            logging.info(word)

    def warning(self, *words):
        for word in words:
            logging.warning(word)

    def error(self, *words):
        for word in words:
            logging.error(word)

    def critical(self, *words):
        for word in words:
            logging.critical(word)

    def write(
        self,
        *words,
    ):

        self._temp_change_formatter(logging.Formatter('%(message)s'))
        try:
            if self.log_name is None:
                self.info(*words)
            else:
                with open(os.path.join(self.home_dir, self.log_name),
                          'a') as log_file:
                    for word in words:
                        log_file.write(word + '\n')
        finally:
            self._restore_original_formatter()

    def _temp_change_formatter(self, new_formatter):
        self.original_formatters = []
        for handler in logging.root.handlers:
            self.original_formatters.append(handler.formatter)
            handler.setFormatter(new_formatter)

    def _restore_original_formatter(self):
        for handler, formatter in zip(logging.root.handlers,
                                      self.original_formatters):
            handler.setFormatter(formatter)


### calculate the end time of model run ###
def get_end_time(start_time: list, duration_str: str) -> list:

    ### parse the duration string ###
    pattern = r'(\d+)([A-Za-z]+)'
    durations = re.findall(pattern, duration_str)

    time_units = {'D': 'days', 'H': 'hours', 'M': 'minutes', 'S': 'seconds'}
    delta = timedelta()
    for amount, unit in durations:
        if unit in time_units:
            kwargs = {time_units[unit]: int(amount)}
            delta += timedelta(**kwargs)

    if delta.total_seconds() == 0:
        return None
    else:
        end_time = []
        for i_time in range(len(start_time)):
            start_time_temp = datetime.strptime(start_time[i_time],
                                                '%Y-%m-%d %H:%M:%S')
            end_time.append(
                (start_time_temp + delta).strftime('%Y-%m-%d %H:%M:%S'))

        time_list = [[a, b] for a, b in zip(start_time, end_time)]

        return time_list


class RunTime:

    def __init__(self) -> None:

        self.methods = {'equal_step': self.equal_step, 'assign': self.assign}

    def get(self, config: dict) -> list:

        self.scheme = config['scheme']
        if not self.scheme in self.methods:
            raise ValueError(
                'Run time generation method : %s is not supported' %
                (self.scheme))

        return self.methods.get(self.scheme)(config[self.scheme])

    ### *--- get the list of all assimilation moments ---* ###
    def equal_step(self, config: dict) -> list:

        asml_time_list = pd.date_range(
            datetime.strptime(config['first_run_time'], '%Y-%m-%d %H:%M:%S'),
            datetime.strptime(config['last_run_time'], '%Y-%m-%d %H:%M:%S'),
            freq=config['asml_time_interval'])

        asml_time_list = asml_time_list.strftime(
            '%Y-%m-%d %H:%M:%S').tolist()  # type : str

        model_time_list = get_end_time(asml_time_list,
                                       config['run_time_range'])

        return asml_time_list, model_time_list

    def assign(self, config: dict) -> list:

        df = pd.read_csv(config['path'])
        asml_time_list = list(df.iloc[:, 0])
        model_time_list = df.iloc[:, 1]
        model_time_list = None if model_time_list.isnull().all() else list(
            model_time_list)

        return asml_time_list, model_time_list


### split the list into few parts ###
def split_list(lst: list, num_parts: int) -> list:

    avg = len(lst) / float(num_parts)
    result = []
    last = 0.0

    while last < len(lst):
        result.append(lst[int(last):int(last + avg)])
        last += avg

    return result


### *--- check and wait until the part finishes ---* ###
def check_status_code(path: str,
                      section: str,
                      key: str,
                      interval=1,
                      check_start=True) -> int:

    def read_status() -> int:
        with open(path, 'r') as f:
            return json.load(f)[section][key]

    def wait_for_start() -> None:
        while read_status() == 0:
            time.sleep(interval)

    def wait_for_finish() -> int:
        status = read_status()
        while status != 100 and status >= 0:
            time.sleep(interval)
            status = read_status()
        return status

    if check_start:
        wait_for_start()

    return wait_for_finish()


### mark the number of the assimilation loop ###
def number_guide(number: int) -> str:

    line='\n######--------------------######\n'+\
         '######      Round {:0>4d}    ######\n'.format(number)+\
         '######--------------------######'

    return line


### *--- introduce marker ---* ###
# generate from "http://patorjk.com/software/taag/#p=display&h=0&v=0&f=Slant&t=PyFilter"
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
