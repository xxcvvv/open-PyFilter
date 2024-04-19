'''
Autor: Mijie Pang
Date: 2023-07-20 15:00:34
LastEditTime: 2024-04-05 15:36:14
Description: 
'''
import os
import sys
from datetime import datetime

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
sys.path.append(os.path.join(main_dir, 'prepare'))

import system_lib as stl


def main(Config: dict, Status: dict, **kwargs):

    ### *--- configure basic settings ---* ###
    assimilation_time = datetime.strptime(
        Status['assimilation']['assimilation_time'], '%Y-%m-%d %H:%M:%S')

    ### *-------------------------------------------* ###
    ### *--- start to download the selected data ---* ###

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


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    Status = stl.read_json(os.path.join(main_dir, 'Status.json'))
    main(Config, Status)
