'''
Autor: Mijie Pang
Date: 2023-09-01 13:57:07
LastEditTime: 2023-09-07 09:45:30
Description: run the ensemble Pangu model using MPI
'''
import os
import sys
import numpy as np
import pandas as pd
from mpi4py import MPI
from datetime import datetime

import Pangu_lib as pgl

sys.path.append('../')
import system_lib as stl

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

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

### generate jobs ###
run_ids = [
    'pangu_%04d' % (i_ensem)
    for i_ensem in range(Config['Model'][model_scheme]['ensemble_number'])
]

### split jobs and assign them to different processes ###
sub_size = int(Config['Model']['node']['max_core_num'] /
               Config['Model']['node']['core_demand'])

run_list = stl.split_list(run_ids, sub_size)[rank]

### initiate the pangu model ###
model = pgl.model_class(model_path,
                        threads=Config['Model']['node']['core_demand'],
                        run_time_range=run_time_range,
                        Config=Config,
                        system_log=system_log)

### allocate a shared memory for output ###
array_shape = (Config['Model'][model_scheme]['ensemble_number'], 4,
               Config['Model'][model_scheme]['nlat'],
               Config['Model'][model_scheme]['nlon'])

if rank == 0:
    win = MPI.Win.Allocate_shared(np.zeros(array_shape,
                                           dtype=np.float64).nbytes,
                                  comm=comm)
else:
    win = MPI.Win.Allocate_shared(0, comm=comm)

shared_buffer, itemsize = win.Shared_query(0)
output_shared = np.ndarray(buffer=shared_buffer,
                           dtype=np.float64,
                           shape=array_shape)

#####################################
###      parallel processing      ###

if rank == 0:
    start_t = datetime.now()

system_log.info('rank : %s ; job list : %s' % (rank, run_list))

for run_id in run_list:

    counter = int(run_id.split('_')[-1])

    model.run(run_id=run_id)

    output_shared[counter, :] = model.pangu.output_surface

system_log.info('jobs all done in rank %s' % (rank))

### synchronize all the processes ###
comm.Barrier()

### end of the script ###
if rank == 0:
    system_log.info('Total process time : {:.2f} s \n'.format(
        (datetime.now() - start_t).total_seconds()))
