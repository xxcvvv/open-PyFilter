'''
Autor: Mijie Pang
Date: 2023-11-30 16:23:13
LastEditTime: 2023-12-05 10:26:48
Description: 
'''
import os
import pandas as pd
from datetime import datetime


def check_model_state(lines: str, depth=10) -> list:

    Finished_flag = False
    Error_flag = False
    finish_mark = ('[INFO    ] End of script at', '[INFO    ] ** end **',
                   '*** end of simulation reached')
    error_mark = ('[ERROR   ] exception', 'subprocess.CalledProcessError:')

    for line in lines[int(-1 * depth):]:

        if line.startswith(finish_mark):
            Finished_flag = True
        elif line.startswith(error_mark):
            Error_flag = True

    return Finished_flag, Error_flag


class Status_Reporter:

    def __init__(self, start_time: str, end_time: str, interval: str,
                 run_dir: str, run_id: str) -> None:

        run_time_range = pd.date_range(start_time, end_time, freq=interval)

        self.run_time_range = run_time_range
        self.run_dir = run_dir
        self.run_id = run_id

        self.progress_index = 0
        self.production_time = datetime.strptime('1900-01-01', '%Y-%m-%d')

    ### *--- check the status of model restart files
    ### by its existence and production time ---* ###
    def check_restart(self, ) -> int:

        ### start from the next not producted files ###
        for time in self.run_time_range[self.progress_index + 1:]:

            restart_file = '%s/restart/LE_%s_state_%s.nc' % (
                self.run_dir, self.run_id, time.strftime('%Y%m%d_%H%M'))

            if os.path.exists(restart_file):

                production_time = os.path.getmtime(restart_file)
                production_time = datetime.fromtimestamp(production_time)
                # print(production_time, self.production_time)

                if production_time > self.production_time:
                    self.progress_index += 1
                    self.production_time = production_time
                else:
                    break

            else:
                break

        return self.progress_index

    ### *--- check the status of model output files
    ### by its existence and production time ---* ###
    # def check_output(self, ) -> int:

    #     for time in self.run_time_range[self.progress_index + 1:]:

    #         output_file = '%s/output/LE_%s_conc-sfc_%s.nc' % (
    #             self.run_dir, self.run_id, time.strftime('%Y%m%d'))

    #         if os.path.exists(output_file):

    #             production_time = os.path.getmtime(output_file)
    #             production_time = datetime.fromtimestamp(production_time)
    #             print(production_time, self.production_time)

    #             if production_time > self.production_time:
    #                 self.progress_index += 1
    #                 self.production_time = production_time
    #             else:
    #                 break

    #         else:
    #             break

    #     return self.progress_index

    ### *--- read the log file to make sure model is running ---* ###
    def check_status(self, ) -> bool:

        log_file_path = '%s/log/%s.out.log' % (self.run_dir, self.run_id)

        with open(log_file_path, 'r') as f:
            lines = f.readlines()

        Finished_flag, Error_flag = check_model_state(lines)

        return Finished_flag, Error_flag


if __name__ == '__main__':

    import time

    SR = Status_Reporter(
        '2023-03-01 00:00', '2023-05-31 23:00', '1H',
        '/home/pangmj/TNO/scratch/projects/Reanalysis/beta072_2023_background',
        'beta072_2023_background')

    for i in range(100):
        SR.check_restart()
        # SR.check_output()
        time.sleep(3)
