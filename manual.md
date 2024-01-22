# Manual of PyFilter
PyFilter is an open-source data assimilation system developped for assimilation-based forecast. To start the system, configure the whole system first (make sure all the scripts are tested in advance). Experimental run and operational forecast are supported in the system. You can type  

## System information
You should fill information about the user and machine in the `Info.json`

```json 
{
"System": {
    "name": "PyFilter",
    "version": 1.1,
    "log_level": "DEBUG",
},
"User": {
    "Author": "",
    "Institute": ""
},
"Machine": {
    "Host": "",
    "management": "SGE"
},
"path": {
    "output_path": "",
    "my_python": ""
}
}
```

These information may be included in the final products. Ensure they are properly stated.  

`log_level` controls the log information. Priorities following `DEBUG -> INFO -> WARNING -> ERROR -> CRITICAL`

`output_path` is the directory where all the results are stored in. 

`my_python` is your own python interpreter to drive the whole system. 

Node management platform supported in the systme is listed below. 

| name  | status           |
| ----- | ---------------- |
| SGE   | fully developed  |
| SLURM | under adaptation |
| PBS   | under adaptation |

## Initial 
This is the first process ran before the rolling assimilation starts. You can install the initial model restart files into the model directory or run the initial model field in this step. The configuration is included in the `Initial.json` in `config` directory. 

```json
"scheme": "install_restart",
"install_restart": {},
"initial_run": {
    "start_time": "2021-03-27 00:00:00",
    "end_time": "2021-03-29 23:00:00",
    "save_restart": true,
    "save_per_hour": 1,
    "backup": false
}
```

Two schemes are included. Assign the shceme in name `scheme` section to specify the method. 

To configure the `initial_run`, see the description below. 
| name            | description                                 |
| --------------- | ------------------------------------------- |
| `start_time`    | start time of the model forecast time range |
| `end_time`      | end time of the model forecast time range   |
| `save_restart`  | choose if save the restart files            |
| `save_per_hour` | time frequency of saveing the restart files |
| `backup`        | if backup the new priori                    |

**The time format must follow the `Year-Month-Day Hour:Minute:Second` rule.**

## Observation 
Assign the observation path first in the `Observation.json`. The same diectory under the system output directory is recommended. Then decide which type of observation is going to be assimilated in the list of `observation2apply`.


```json
"path": "/home/pyFilter/observation",
"observation2apply": [
        "ground_station"
    ]
```

There are some typical kinds of observation data listed in the file. Each catagory of the data is stored under the `path/dir` as named in the configure.

```json
"ground_station": {
    "dir": "Ass_PM10_UTC",
    "product": "BC_PM10",
    "description": "",
    "source": "MEE"
},
"modis": {
    "dir": "",
    "product": "AOD",
    "description": "",
    "source": "NASA"
}
```

## Model
PyFilter is designed for multiple models. Currently, several models are adapted, see the list below. The model source files are not included in the system. Instead, it is linked to the system externally. Models can be easily linked to the PyFilter with minimum modification. The model configuration can be found and edited in the `Model.json`. 

Assign the `name` to specify the model used. 

```json
"scheme": {
    "name": "lotos-euros",
    "run": "node"
}
```

The current model working list. 
| name        | status                   |
| ----------- | ------------------------ |
| LOTOS-EUROS | v2.1 and v2.2 is adapted |
| WRF-GC      | working on               |
| Pangu       | working on               |
| WRF-Chem    | future plan              |

### Node arrangement
All the models supported are ran on the node.

Currently, only submission to *SGE* is fully developed. Manual and autonomous node submission is both supported.  

```json
"node": {
        "gpu": false,
        "node_id": "7",
        "core_demand": 4,
        "auto_node_selection": true,
        "random_node": false,
        "max_core_num": 8
}
```

`node_id` is to manually assign the node number to submit.  

`core_demand` is to assign how manys cores are needed for every ensemble.  

Set `auto_node_selection = true` to enable the autonomous node selection feature. In this selection, the system will find all the available node to submit. The node black list can be added in `start_run_lib.py` -> `get_available_node` so that the node in black list won't be included.   

`max_core_num` is to declare the maximum core number you can use. So the system can limit the total core usage. 

### Path settings
The path settings are attatched in every seperated model settings. Add it under the specific model. Here, some paths concerning the model should be assigned.

```json
"path": {
    "model_path": "",
    "model_bashrc_path": "",
    "model_output_path": "",
    "backup_path": ""
}
```

See the list below with the descriptions. 
| name                | description                                       |
| ------------------- | ------------------------------------------------- |
| `model_path`        | where the model script located                    |
| `model_bashrc_path` | where the enviroment variables are stored         |
| `model_output_path` | where the model is assigned to produce the output |
| `backup_path`       | where the backup of the model priori is stored    |

### **LOTOS-EUROS** model settings 
```json
"lotos-euros": {
    "path":{},
    "run_project": "test",
    "version": "v2.2",
    "run_type": "ensemble",
    "run_method": "batch",
    "max_retry": 8,
    "ensemble_number": 32,
    "model_first_run_time": "20xx-xx-xx xx:xx:xx",
    "model_last_run_time": "20xx-xx-xx xx:xx:xx",
    "running_time_range": "24H",
    "start_from_restart": true,
    "nspec": 5,
    "nlevel": 99,
    "start_lon": 0,
    "end_lon": 10,
    "res_lon": 0.1,
    "nlon": 100,
    "start_lat": 0,
    "end_lat": 10,
    "res_lat": 0.1,
    "nlat": 100,
    "iteration_num": 1,
    "output_time_interval": "1H"
}
```
The latest version of *LOTOS-EUROS* currently used is v2.2. The project of model run will be named after `run_project`. 

There are three types of ensemble model running strategy can be set in `run_type`:  

* `ensemble`: all the model ensembles run in parallel. The number of ensembles is stead throughout the forecast period

* `single`: combine all the ensembles after the initial assimilation analysis to an average and run the forecast from this single average  

* `ensemble_extend`: designed for NTEnKF. The number of ensembles will be extended as the *NTEnKF* is assigned and configured.

There are two running methods suppoted for ensemble model in `run_method`:

* `set`: all the ensemble will be submittd at the same time in a determined set. if the resource is not enough, system will wait until there is available. 

* `batch`: all the ensembles will be put into a pool and run when there is still resource available. This method is preferable when there isn't enough resource. 

`model_first_run_time` determine the first start time of the model forecast restarted from the assimilation analysis and `model_last_run_time` determines the last of it. *If they are the same, the model forecast will run only one time.*

`running_time_range` is the model forecast time range after every assimilation analysis. Set it as `0H` is to run assimilation analysis solely and model is skipped. Suffix of 'H' or 'D' is necessary and supported. Rest of settings are for model confifuration. 

### post processing 
```json
"post_process": {
    "save_method": "origin",
    "save_tool": "mv",
    "data_type": "output",
    "plot_results": true,
    "product": [
        "standard"
    ],
    "run_spec": [
        "gate",
        16
    ],
    "plot_forecast": true,
    "with_observation": true
}
```
Here is to assign the data processing after the model forecast. Procedure of saving the model output is counted in system process and the rest is not necessary part. 

There are 3 kinds of save method supported now:
* `none`: do nothing to the model output and just skip to the next process. 
* `origin`: save the model output to the destinated directory. Which is `Assmilation['path']['results_path']+'/'+run_project+'/'+project_name+'/'+forecast+'/'+forecast-start-time`
* `merge`: combine all the model output to generate the product. if it is selected, the system will do `origin` procedure first and produce the final product based on the original files. You can design the product in scripts and select the product name in `product`.

`save_tool`: only `mv` and `rsync` is supported. `mv` is prefered for it is much faster when the model output is large. 

`plot_results` is to determine if to plot the results. Set it to false will turn off all the plot jobs. `run_spec` is to assign the resource needed to plot. It is a list contains `gate` or `node` and core number to use. 

## Assimilation analysis
The PyFilter is capable of various assimilation algorithms including EnKF, 3DVar. To begin with, fill the `Assimilation.json` in `config` directory. 
Assign the assimilation `scheme` and decide if the assimilation runs on `node` or `gate`.

```json 
"scheme": {
    "name": "ntenkf_hybrid",
    "run": "node"
}
```

Support list of assmilation algorithms can be seen below.

| name   | status     |
| ------ | ---------- |
| EnKF   | supported  |
| NTEnKF | supported  |
| 3DVar  | working on |

The LEnKF is not listed seperately for the localization is implemented in all the supported algorithms. 

### Node arrangement
The node submission is also supported for assimilation algorithms. As same as the model submission, autonomous node selection is also supported. Here is the sample and description.

```json
"node": {
    "auto_node_selection": true,
    "node_id": "1",
    "core_demand": 16
}
```

| name                | description                                            |
| ------------------- | ------------------------------------------------------ |
| auto_node_selection | decide if autonomous select the node id                |
| node_id             | assign the node id manually                            |
| core_demand         | assign the number of core remanded in the assimilation |

### Assimilation algorithms
When configuring the assimilation settings, name the assimilation project (`project_name`) firstly. This will create the sub-directory under the model `run_project`. And they compose the entire system project directory. 

`time_interal` determines the assimilation time frequency during the whole assimialtion period. Suffix of `H` or `D` is supported. 

`write_restart` should be set as ture unless you just want to run a pure assimilation analysis test and don't want to mess up the model restart files. 

#### EnKF

```json 
"enkf": {
    "project_name": "test",
    "write_restart": true,
    "use_localization": true,
    "distance_threshold": 500, 
    "time_interval": "3H"
}
```

To ensble the localization, set `use_localization` as ture and the localization distance threshold can be set in `distance_threshold`. The unit is *km*.

#### NTEnKF
NTEnKF algorithm runs at a hybrid way.    

```json
"ntenkf_hybrid": {
    "project_name": "test",
    "write_restart": true,
    "use_localization": false,
    "distance_threshold": 500,
    "execute_time_point": "2021-03-15 08:00:00",
    "time_interval": "3H",
    "ensemble_set": [
        32,
        32,
        32
    ],
    "time_set": [
        -2,
        0,
        2
    ]
}
```  

`NTEnKF_hybrid` is is to do NTEnKF analysis at the initial assimilation time. After the initial NTEnKF analysis, the number of ensembles is extended and run as the new priori. In the following assimilation time, EnKF is conducted. You can assign the ensemble number used in the neighboring time choice in `ensemble_set` and `time_set`.