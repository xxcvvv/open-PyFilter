'''
Autor: Mijie Pang
Date: 2023-08-15 18:09:22
LastEditTime: 2023-09-23 09:19:34
Description: 
'''
import os
import cdsapi
import concurrent.futures
from datetime import datetime


class era5_downloader:

    ### initialization ###
    def __init__(self,
                 url='https://cds.climate.copernicus.eu/api/v2',
                 key='',
                 product_type='reanalysis',
                 time=datetime.now(),
                 format='netcdf',
                 extent=[
                     90,
                     0,
                     -90,
                     359.75,
                 ],
                 output_dir='.',
                 **kwargs) -> None:

        ### generate the time range ###
        if 'time_range' in kwargs.keys():
            self.time_range = kwargs['time_range']
        else:
            self.time_range = [time]

        ### initialize the client ###
        self.client = cdsapi.Client(url=url, key=key)

        self.product_type = product_type
        self.format = format
        self.extent = extent  # start_lat, start_lon, end_lat, end_lon
        self.output_dir = output_dir

    ### download the data ###
    def downloader(self, time, product: str, var: str, **kwargs) -> None:

        request_dict = {
            'variable': var,
            'product_type': self.product_type,
            'year': time.strftime('%Y'),
            'month': time.strftime('%m'),
            'day': time.strftime('%d'),
            'time': time.strftime('%H:%S'),
            'area': self.extent,
            'format': self.format
        }
        request_dict.update(kwargs['additional'])
        print(request_dict)
        # count the time #
        start = datetime.now()

        self.client.retrieve(
            product, request_dict, '%s/%s/ERA5_%s_%s.nc' %
            (self.output_dir, product, var, time.strftime('%Y%m%d_%H%M')))

        print('Download took : %.2f s' % (datetime.now() - start).total_seconds())

    ### execute the downloader in sequence ###
    def retrieve(self, product: str, var_list: list, **kwargs) -> None:

        ### create output directories ###
        if not os.path.exists(self.output_dir + '/' + product):
            os.makedirs(self.output_dir + '/' + product)

        for time in self.time_range:
            for var in var_list:
                self.downloader(time, product, var, additional=kwargs)

    ### execute the downloader in multi threads ###
    def retrieve_threads(self,
                         product: str,
                         var_list: list,
                         threads=6,
                         **kwargs) -> None:

        ### create output directories ###
        if not os.path.exists(self.output_dir + '/' + product):
            os.makedirs(self.output_dir + '/' + product)

        ### open the multi threads ###
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=threads) as executor:

            tasks = []
            for time in self.time_range:
                for var in var_list:
                    tasks.append(
                        executor.submit(self.downloader,
                                        time,
                                        product,
                                        var,
                                        additional=kwargs))

            concurrent.futures.wait(tasks)
