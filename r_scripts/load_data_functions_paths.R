#Load functions
source('thresholds-master/UPAS_thresholds.R')
source('thresholds-master/lascar_thresholds.R')
source('thresholds-master/ecm_thresholds.R')
source('r_scripts/UPAS_functions.R')
source('r_scripts/lascar_functions.R')
source('r_scripts/ecm_functions.R')
source('r_scripts/pats_functions.R')
source('r_scripts/merging_functions.R')
source('r_scripts/beacon_functions.R')
source('r_scripts/generic_functions.R')
source('r_scripts/tsi_functions.R')
source('r_scripts/plots.r')
mobenzi_import_fun()
equipment_IDs_fun
lascar_cali_import()

#Load paths
path_other <- "~/Dropbox/UNOPS emissions exposure/Data/Data from the team/Excel databases/Other databases.xlsx"
path_emissions <- "~/Dropbox/UNOPS emissions exposure/Data/Data from the team/Excel databases/USE THIS COPY_2019 Kenya Emissions database_Updated March 18.xlsx"

tsifilepath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Cleaned TSI Data" #the corrected files should be placed here.
file_list_tsi <- list.files(tsifilepath, pattern='.csv|.CSV', full.names = T,recursive = T)

upasfilepath <- "~/Dropbox/UNOPS emissions exposure/Data/Data from the team/UPAS" 
file_list_upas <- list.files(upasfilepath, pattern='.txt|.TXT', full.names = T,recursive = F)

patsfilepath <- "~/Dropbox/UNOPS emissions exposure/Data/Data from the team/PATS+"
file_list_pats <- list.files(patsfilepath, pattern='.csv|.CSV', full.names = T,recursive = T)

lascarfilepath <- "~/Dropbox/UNOPS emissions exposure/Data/Data from the team/Lascar/Data Files" 
file_list_lascar <- list.files(lascarfilepath, pattern='.txt|.TXT|.csv', full.names = T,recursive = T)
lascarfilepath_caa <- "~/Dropbox/UNOPS emissions exposure/Data/CAA Data/LASCAR Files"
file_list_lascar_caa <- list.files(lascarfilepath_caa, pattern='.txt|.TXT|.csv', full.names = T,recursive = F)

file_list_micropem <- list.files("~/Dropbox/CA(Kenya)/Clean Air Africa/MicroPEM and ECM Raw Files", pattern='.csv|.CSV', full.names = T,recursive = T)
file_list_ECM <- list.files("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/ECM:MicroPEM Processed Data", pattern='.csv|.CSV', full.names = T,recursive = T)
beaconfilepath <- "~/Dropbox/UNOPS emissions exposure/Data/Data from the team/Beacon Logger"
file_list_beacon <- list.files(beaconfilepath, pattern='.csv|.CSV', full.names = T,recursive = T)

#Load data
preplacement <- as.data.table(readRDS("Processed Data/preplacement.rds"))
postplacement <- readRDS("Processed Data/postplacement.rds")
mobenzi_indepth <- readRDS("Processed Data/mobenzi_indepth.rds")
mobenzi_rapid <- readRDS("Processed Data/mobenzi_rapid.rds")
equipment_IDs <- readRDS("Processed Data/equipment_IDs.rds")
lascar_cali_coefs <- readRDS("Processed Data/lascar_calibration_coefs.rds")
CO_calibrated_timeseries <- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/CO_calibrated_timeseries.rds")
pats_data_timeseries <- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/pats_data_timeseries.rds")
tsi_timeseries <- as.data.table(readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/tsi_timeseries.rds"))
beacon_logger_data<- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/beacon_logger_data.rds")
beacon_logger_COmerged<- as.data.table(readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/beacon_logger_COmerged.rds"))
upasmeta<- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/upasmeta.rds")
beacon_meta_qaqc<-readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/beacon_meta_qaqc.rds")
lascar_meta<-readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/lascar_meta.rds")
tsi_meta_qaqc<-readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/tsi_meta_qaqc.rds")
pats_meta_qaqc<-readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/pats_meta_qaqc.rds")

todays_date <- gsub("-", "", as.character(Sys.Date()))
local_tz = "Africa/Nairobi"

# Excel metadata import
metadata_filter_record <- upas_record_import_fun(path_other, sheetname='UPAS Filter record')
meta_emissions <- emissions_import_fun(path_emissions,sheetname='Ambient Sampling',local_tz); assign("meta","meta_emissions", envir=.GlobalEnv)
metadata_ambient <- ambient_import_fun(path_other,sheetname='Ambient Sampling')







