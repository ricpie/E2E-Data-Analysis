

#Load functions
source('thresholds-master/UPAS_thresholds.R')
source('thresholds-master/lascar_thresholds.R')
source('thresholds-master/ecm_thresholds.R')
source('r_scripts/UPAS_functions.R')
source('r_scripts/lascar_functions.R')
source('r_scripts/ecm_functions.R')
source('r_scripts/pats_functions.R')
source('r_scripts/beacon_functions.R')
source('r_scripts/generic_functions.R')
source('r_scripts/tsi_functions.R')
source('r_scripts/plots.r')
mobenzi_import_fun()
equipment_IDs_fun
lascar_cali_import()

#Load paths
path_other <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/Excel databases/Other databases.xlsx"
path_emissions <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/Excel databases/USE THIS COPY_2019 Kenya Emissions database_Updated Jan 28.xlsx"
tsifilepath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Cleaned TSI Data" #the corrected files should be placed here.
file_list_tsi <- list.files(tsifilepath, pattern='.xls|.XLS', full.names = T,recursive = T)
upasfilepath <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/UPAS" 
file_list_upas <- list.files(upasfilepath, pattern='.txt|.TXT', full.names = T,recursive = F)
patsfilepath <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/PATS+"
file_list_pats <- list.files(patsfilepath, pattern='.csv|.CSV', full.names = T,recursive = T)
lascarfilepath <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/Lascar" 
lascarfilepath_caa <- "~/Dropbox/CA(Kenya)/Clean Air Africa/LASCAR Files"
file_list_lascar <- list.files(lascarfilepath, pattern='.txt|.TXT', full.names = T,recursive = T)
file_list_lascar_caa <- list.files(lascarfilepath_caa, pattern='.txt|.TXT', full.names = T,recursive = F)
file_list_micropem <- list.files("~/Dropbox/CA(Kenya)/Clean Air Africa/MicroPEM and ECM Raw Files", pattern='.csv|.CSV', full.names = T,recursive = T)
file_list_PATS <- list.files("~/Dropbox/CA(Kenya)/UNOPs/Data from the team/PATS+", pattern='.csv|.CSV', full.names = T,recursive = T)
file_list_ECM <- list.files("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/ECM:MicroPEM Processed Data", pattern='.csv|.CSV', full.names = T,recursive = T)
beaconfilepath <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/Beacon Logger"
file_list_beacon <- list.files(beaconfilepath, pattern='.csv|.CSV', full.names = T,recursive = T)


#Load data
preplacement <- as.data.table(readRDS("Processed Data/preplacement.R"))
postplacement <- readRDS("Processed Data/postplacement.R")
mobenzi_indepth <- readRDS("Processed Data/mobenzi_indepth.R")
mobenzi_rapid <- readRDS("Processed Data/mobenzi_rapid.R")
equipment_IDs <- readRDS("Processed Data/equipment_IDs.R")
lascar_cali_coefs <- readRDS("Processed Data/lascar_calibration_coefs.R")
CO_calibrated_timeseries <- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/CO_calibrated_timeseries.rds")
pats_data_timeseries <- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/pats_data_timeseries.rds")
tsi_timeseries <- as.data.table(readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/tsi_timeseries.rds"))
beacon_data_timeseries<- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Beacon_RawData.rds")
beacon_logger_COmerged<- as.data.table(readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/beacon_logger_COmerged.rds"))

todays_date <- gsub("-", "", as.character(Sys.Date()))
local_tz = "Africa/Nairobi"
processed_filelist <- "c" #readLines('Processed Data/processed_filelist.csv') #uncomment to speed up, once fully debugged.
averaging_window = 1 #Minutes.  Use 1 or 5 generally.

# Excel metadata import
metadata_filter_record <- upas_record_import_fun(path_other, sheetname='UPAS Filter record')
meta_emissions <- emissions_import_fun(path_emissions,sheetname='Ambient Sampling',local_tz); assign("meta","meta_emissions", envir=.GlobalEnv)
metadata_ambient <- ambient_import_fun(path_other,sheetname='Ambient Sampling')

