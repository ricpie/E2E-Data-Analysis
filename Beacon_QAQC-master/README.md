# Introduction
Quality check for Beacon logger data, with input data from Box, Redcap and Beacon emitter inventory. The data is based on B1 data from India IRC, if extending to use data from other IRC, changes of scripts should be expected.
This QAQC is conducted by each Beacon logger, 6 flags (completeness_fl, time_interval_fl, beacon_presence_fl, beacon_time, startup_proc_fl, duration_fl) were checked.

## Script
### Beacon_QAQC_script.R
Well annocated scripts contains the code to conduct qality check of Beacon logger data from Box, cross-checked with H41 Redcap form.
### load.R
Load packages needed for Beacon_QAQC_script.R

## Data
### Beacon_logger_data
Folder containning Beacon logger files downloaded from Box, in .csv format.
### redcap_data
Folder containning Redcap H41 form, in .xlsx format as exported from Redcap web server.
### Beacon_emitter_id_230619.csv
File containning information of ID (last five diginits) and MAC address of Beacon emitters used in India IRC.

## Output results
beacon_qa_20190808.csv is the output results containning QC information for Beacon logger.

# Beacon QAQC criteria

## Completeness flag
If the data is less than 6 hours for this logger, completeness flag will be trigerred
## Time interval flag
If the time interval for data logged in this logger is not in the arange between [15, 45], time interval flag is trigerred.
## Beacon presence flag
If for this logger, less than 4 beacon emitters are logged, Beacon presece flag will be set to 1. Please be noted sometimes less/more than 4 emitters will be used in deployment, this flags need to be revised.
At least 1 of children and mothers' beacon emitter should be presented in the logger data, if not, beacon presence flag is set to 2.
## Beacon time flag
Data from all Beacon emitters deployed should be visible for at least 1 hour of deployment in this logger, if not, Beacon time flag will be set to 1.
## startup procedure flag
If the maxium RSSI (received signal strength indicator)is less than -75 (loggers are placed to further away or obstruction), start up procedure flag is trigger, indicating loggers are placed too far from emitters.
## duration flag
Loggers should logged data between 22 - 26 hours, if not, duration flag is triggerred. This indicates failures of logger batery packs.
