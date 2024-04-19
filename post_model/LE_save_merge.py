'''
Autor: Mijie Pang
Date: 2023-02-20 16:23:33
LastEditTime: 2024-04-04 17:43:50
Description: 
'''
import os
import sys
import logging
import numpy as np
import pandas as pd
import netCDF4 as nc

main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_dir)
import system_lib as stl
from tool.pack import NcProduct


def main(Config: dict, Status: dict):

    ### *--- initiate parameters ---* ###
    model_scheme = Config['Model']['scheme']['name']
    assi_scheme = Config['Assimilation']['scheme']['name']
    iteration_num = Config['Model'][model_scheme]['iteration_num']
    ensemble_number = Config['Model'][model_scheme]['ensemble_number']

    time_range = pd.date_range(
        Status['model']['start_time'],
        Status['model']['end_time'],
        freq=Config['Model'][model_scheme]['output_time_interval'])

    data_dir = os.path.join(
        Config['Info']['path']['output_path'],
        Config['Model'][model_scheme]['run_project'],
        Config['Assimilation'][assi_scheme]['project_name'], 'forecast',
        time_range[0].strftime('%Y%m%d_%H%M'), 'forecast_files')
    dust_bin = ['dust_ff', 'dust_f', 'dust_c', 'dust_cc', 'dust_ccc']

    ### *--- define the run ids ---* ###
    if Config['Model'][model_scheme]['run_type'] == 'ensemble':

        run_ids = [
            'iter_%02d_ensem_%02d' % (iteration_num, i_ensemble)
            for i_ensemble in range(ensemble_number)
        ]

    elif Config['Model'][model_scheme]['run_type'] == 'ensemble_extend':

        time_set = Config['Assimilation'][assi_scheme]['time_set']
        ensemble_set = Config['Assimilation'][assi_scheme]['ensemble_set']
        run_ids = [[
            't_' + str(time_set[i_time]) + '_e_%02d' % (i_ensem)
            for i_ensem in range(int(ensemble_set[i_time]))
        ] for i_time in range(len(time_set))]
        run_ids = np.ravel(np.array(run_ids))

    logging.debug(f'list of run id : {run_ids}')

    ### *-------------------------------------------* ###
    ### *                                           * ###
    ### *    Start to produce the output product    * ###
    ### *                                           * ###
    ### *-------------------------------------------* ###

    logging.debug('Product list : %s' %
                  (Config['Model'][model_scheme]['post_process']['product']))

    ### *--------------------------------------------------------------* ###
    ### *---       Product 1 : the 3d dust concentration data       ---* ###
    if 'conc-3d' in Config['Model'][model_scheme]['post_process']['product']:

        logging.debug('start to produce dust 3d concentration product')

        ### initialize the netcdf product ###
        nc_product = NcProduct(data_dir + '/dust_conc-3d.nc',
                               Model=Config['Model'],
                               Assimilation=Config['Assimilation'],
                               Info=Config['Info'])
        nc_product.define_dimension(
            longitude=Config['Model'][model_scheme]['nlon'],
            latitude=Config['Model'][model_scheme]['nlat'],
            level=Config['Model'][model_scheme]['nlevel'],
            spec=Config['Model'][model_scheme]['nspec'],
            time=None)
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            time=['S19', 'time'],
            altitude=['f4', ('level', 'latitude', 'longitude')],
            dust_conc=[
                'f4', ('time', 'spec', 'level', 'latitude', 'longitude')
            ])

        ### define some basic variables ###
        source_dir = os.path.join(data_dir, run_ids[0], 'output')
        with nc.Dataset(
                os.path.join(
                    source_dir, 'LE_%s_conc-3d_%s.nc' %
                    (run_ids[0], time_range[0].strftime('%Y%m%d')))) as nc_obj:
            nc_product.add_data(longitude=nc_obj.variables['longitude'][:])
            nc_product.add_data(latitude=nc_obj.variables['latitude'][:])
            nc_product.add_data(altitude=nc_obj.variables['altitude'][0, :])

        nc_product.close()

        ### read the model forecast output ###
        for i_time in range(len(time_range)):

            # print(time_range[i_time])

            conc_3d = np.zeros([
                Config['Model'][model_scheme]['nspec'],
                Config['Model'][model_scheme]['nlevel'],
                Config['Model'][model_scheme]['nlat'],
                Config['Model'][model_scheme]['nlon']
            ])

            for i_run in range(len(run_ids)):

                source_dir = data_dir + '/' + run_ids[i_run] + '/output'
                # print(run_ids[i_run])
                with nc.Dataset(source_dir + '/LE_' + run_ids[i_run] +
                                '_conc-3d_' +
                                time_range[i_time].strftime('%Y%m%d') +
                                '.nc') as nc_obj:

                    output_time = nc_obj.variables['time']
                    output_time = nc.num2date(output_time[:],
                                              output_time.units)
                    time_idx = np.where(
                        output_time == time_range[i_time])[0][0]

                    for i_spec in range(len(dust_bin)):
                        conc_3d[i_spec, :] += nc_obj.variables[
                            dust_bin[i_spec]][time_idx, :]

            conc_3d = conc_3d / len(run_ids) * (10**9)

            ### save the results to netcdf file ###
            nc_product = NcProduct(data_dir + '/dust_conc-3d.nc', mode='a')
            nc_product.add_data(
                time=time_range[i_time].strftime('%Y-%m-%d %H:%M:%S'))
            nc_product.add_data(count=i_time, dust_conc=conc_3d)
            nc_product.close()

        logging.debug('Production finished')

    # end of product 1
    ### *--------------------------------------------------------------* ###

    ### *-------------------------------------------------------------* ###
    ### *---                    Product 2 : AOD                    ---* ###
    if 'aod2' in Config['Model'][model_scheme]['post_process']['product']:

        logging.debug('Start to produce dust aod product')

        ### initialize the netcdf product ###
        nc_product = NcProduct(data_dir + '/dust_aod.nc',
                               Model=Config['Model'],
                               Assimilation=Config['Assimilation'],
                               Info=Config['Info'])
        nc_product.define_dimension(
            longitude=Config['Model'][model_scheme]['nlon'],
            latitude=Config['Model'][model_scheme]['nlat'],
            time=None)
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            time=['S19', 'time'],
            aod_550nm=['f4', ('time', 'latitude', 'longitude')])

        source_dir = data_dir + '/' + run_ids[0] + '/output'
        with nc.Dataset(
                os.path.join(
                    source_dir, 'LE_%s_aod2_%s.nc' %
                    (run_ids[0], time_range[0].strftime('%Y%m%d')))) as nc_obj:
            nc_product.add_data(longitude=nc_obj.variables['longitude'][:])
            nc_product.add_data(latitude=nc_obj.variables['latitude'][:])

        nc_product.close()

        ### read the model forecast output ###
        for i_time in range(len(time_range)):

            # print(time_range[i_time])

            dust_aod = np.zeros([
                Config['Model'][model_scheme]['nlat'],
                Config['Model'][model_scheme]['nlon']
            ])

            for i_run in range(len(run_ids)):

                source_dir = data_dir + '/' + run_ids[i_run] + '/output'
                # print(run_ids[i_run])
                with nc.Dataset(
                        os.path.join(
                            source_dir, 'LE_%s_aod2_%s.nc' %
                            (run_ids[i_run], time_range[i_time].strftime(
                                '%Y%m%d')))) as nc_obj:

                    output_time = nc_obj.variables['time']
                    output_time = nc.num2date(output_time[:],
                                              output_time.units)
                    time_idx = np.where(
                        output_time == time_range[i_time])[0][0]

                    dust_aod[:] += nc_obj.variables['aod_550nm'][time_idx, :]

            dust_aod = dust_aod / len(run_ids)

            ### save the results to netcdf file ###
            nc_product = NcProduct(data_dir + '/dust_aod.nc', mode='a')
            nc_product.add_data(
                time=time_range[i_time].strftime('%Y-%m-%d %H:%M:%S'))
            nc_product.add_data(count=i_time, aod_550nm=dust_aod)
            nc_product.close()

        logging.debug('Production finished')

    ### end of product 2
    ### *-------------------------------------------------------------* ###

    ### *---------------------------------------------------------------* ###
    ### *---     Product 3 : the surface dust concentration data     ---* ###
    if 'conc-sfc' in Config['Model'][model_scheme]['post_process']['product']:

        logging.debug('Start to produce dust surface concentration product')

        ### initialize the netcdf product ###
        nc_product = NcProduct(data_dir + '/dust_conc-sfc.nc',
                               Model=Config['Model'],
                               Assimilation=Config['Assimilation'],
                               Info=Config['Info'])
        nc_product.define_dimension(
            longitude=Config['Model'][model_scheme]['nlon'],
            latitude=Config['Model'][model_scheme]['nlat'],
            spec=Config['Model'][model_scheme]['nspec'],
            time=None)
        nc_product.define_variable(
            longitude=['f4', 'longitude'],
            latitude=['f4', 'latitude'],
            time=['S19', 'time'],
            dust_conc=['f4', ('time', 'spec', 'latitude', 'longitude')])

        source_dir = data_dir + '/' + run_ids[0] + '/output'
        with nc.Dataset(
                os.path.join(
                    source_dir, 'LE_%s_conc-sfc_%s.nc' %
                    (run_ids[0], time_range[0].strftime('%Y%m%d')))) as nc_obj:
            nc_product.add_data(longitude=nc_obj.variables['longitude'][:])
            nc_product.add_data(latitude=nc_obj.variables['latitude'][:])
        nc_product.close()

        ### read the model forecast output ###
        for i_time in range(len(time_range)):

            # print(time_range[i_time])

            conc_sfc = np.zeros([
                Config['Model'][model_scheme]['nspec'],
                Config['Model'][model_scheme]['nlat'],
                Config['Model'][model_scheme]['nlon']
            ])

            for i_run in range(len(run_ids)):

                source_dir = data_dir + '/' + run_ids[i_run] + '/output'
                # print(run_ids[i_run])
                with nc.Dataset(
                        os.path.join(
                            source_dir, 'LE_%s_conc-sfc_%s.nc' %
                            (run_ids[i_run], time_range[i_time].strftime(
                                '%Y%m%d')))) as nc_obj:

                    output_time = nc_obj.variables['time']
                    output_time = nc.num2date(output_time[:],
                                              output_time.units)
                    time_idx = np.where(
                        output_time == time_range[i_time])[0][0]

                    for i_spec in range(len(dust_bin)):
                        conc_sfc[i_spec, :] += nc_obj.variables[
                            dust_bin[i_spec]][time_idx, 0, :, :]

            conc_sfc = conc_sfc / len(run_ids) * (10**9)

            ### save the results to netcdf file ###
            nc_product = NcProduct(data_dir + '/dust_conc-sfc.nc', mode='a')
            nc_product.add_data(
                time=time_range[i_time].strftime('%Y-%m-%d %H:%M:%S'))
            nc_product.add_data(count=i_time, dust_conc=conc_sfc)
            nc_product.close()

        logging.debug('Production finished')

    ### end of product 3
    ### *---------------------------------------------------------------* ###


if __name__ == '__main__':

    stl.Logging()
    Config = stl.read_json_dict(os.path.join(main_dir, 'config'), get_all=True)
    Status = stl.read_json(path=os.path.join(main_dir, 'Status.json'))
    main(Config, Status)
