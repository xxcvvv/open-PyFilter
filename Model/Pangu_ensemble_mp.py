'''
Autor: Mijie Pang
Date: 2023-08-29 20:21:37
LastEditTime: 2023-09-02 20:47:50
Description: run the ensemble Pangu model using multiprocessing
'''
import os
import sys
import pandas as pd
import multiprocessing as mp
from datetime import datetime

import Pangu_lib as pgl

sys.path.append('../')
import system_lib as stl

config_dir = '../config'
status_path = '../Status.json'

### read system configuration ###
Config = stl.read_json_dict(config_dir, get_all=True)
Status = stl.read_json(path=status_path)

model_scheme = Config['Model']['scheme']['name']
assimilation_scheme = Config['Assimilation']['scheme']['name']

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### initiate parameters ###
model_path = os.path.join(
    Config['Model']['pangu']['path']['model_path'], 'pangu_weather_%s.onnx' %
    (Config['Model'][model_scheme]['output_time_interval'][:-1]))

run_time_range = pd.date_range(
    Status['model']['start_time'],
    Status['model']['end_time'],
    freq=Config['Model'][model_scheme]['output_time_interval'])

run_ids = [
    'pangu_%04d' % (i_ensem)
    for i_ensem in range(Config['Model'][model_scheme]['ensemble_number'])
]

### prepare the job list ###
job_info = {
    'model_path': model_path,
    'Config': Config,
    'Status': Status,
    'run_time_range': run_time_range
}
job_list = []
for i_run in range(len(run_ids)):
    job_info.update({'run_id': run_ids[i_run]})
    job_list.append(job_info.copy())

#################################
###    parallel processing    ###
start_t = datetime.now()

process_num = int(Config['Model']['node']['max_core_num'] /
                  Config['Model']['node']['core_demand'])
pool = mp.Pool(process_num)

results = [pool.apply_async(pgl.run, kwds={'info': job}) for job in job_list]
results = [p.get() for p in results]

system_log.info('Total process time : {:.2f} s'.format(
    (datetime.now() - start_t).total_seconds()))
