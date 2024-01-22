'''
Autor: Mijie Pang
Date: 2023-07-20 15:00:34
LastEditTime: 2023-09-08 19:37:43
Description: 
'''
import os
import sys
from datetime import datetime
import download_lib as dol

sys.path.append('../')
import system_lib as stl

config_dir = '../config'
status_path = '../Status.json'

### read configuration ###
Config = stl.read_json_dict(config_dir, get_all=True)
Status = stl.read_json(path=status_path)

### prepare log class ###
system_log = stl.Logging(os.path.join(
    Status['system']['home_dir'], Status['system']['system_project'] + '.log'),
                         level=Config['Info']['System']['log_level'])

### configure basic settings ###
assimilation_time = datetime.strptime(
    Status['assimilation']['assimilation_time'], '%Y-%m-%d %H:%M:%S')

#################################################
###    start to download the selected data    ###

### ERA5 reanalysis data ###
if 'era5' in Config['Input']['input_list']:

    import ERA5_lib as erl

    era5_worker = erl.era5_downloader(
        url=Config['Input']['era5']['api']['url'],
        key=Config['Input']['era5']['api']['key'],
        time=assimilation_time,
        extent=Config['Input']['era5']['extent'],
        output_dir=Config['Input']['path'])

    ### product type 1 : reanalysis-era5-single-levels ###
    product_type = 'reanalysis-era5-single-levels'
    if product_type in Config['Input']['era5']['product'].keys():

        era5_worker.retrieve_threads(
            product_type,
            Config['Input']['era5']['product'][product_type]['vars'])

    ### product type 2 : reanalysis-era5-pressure-levels ###
    product_type = 'reanalysis-era5-pressure-levels'
    if product_type in Config['Input']['era5']['product'].keys():

        era5_worker.retrieve_threads(
            product_type,
            Config['Input']['era5']['product'][product_type]['vars'],
            pressure_level=[
                'Input'
            ]['era5']['product'][product_type]['pressure_level'])
