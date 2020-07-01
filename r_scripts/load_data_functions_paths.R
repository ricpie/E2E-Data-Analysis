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
source('r_scripts/ses_pca_function.R')
source('r_scripts/tsi_functions.R')
source('r_scripts/models_e2e.R')
source('r_scripts/plots.r')
mobenzi_import_fun()
equipment_IDs_fun()
lascar_cali_import()
local_tz = "Africa/Nairobi"


#Load paths
path_other <- "../Data/Data from the team/Excel databases/Other databases.xlsx"
path_emissions <- "../Data/Data from the team/Excel databases/2019 E2E Emissions database_v2.xlsx"
emissions_import_fun(path_emissions,'Data',local_tz)

tsifilepath <- "Processed Data/Cleaned TSI Data" #the corrected files should be placed here.
file_list_tsi <- list.files(tsifilepath, pattern='.csv|.CSV', full.names = T,recursive = T)

upasfilepath <- "../Data/Data from the team/UPAS" 
file_list_upas <- list.files(upasfilepath, pattern='.txt|.TXT', full.names = T,recursive = F)

patsfilepath <- "../Data/Data from the team/PATS+/HAP P+"
file_list_pats <- list.files(patsfilepath, pattern='.csv|.CSV', full.names = T,recursive = F)

lascarfilepath <- "../Data/Data from the team/Lascar/Data Files" 
file_list_lascar <- list.files(lascarfilepath, pattern='.txt|.TXT|.csv', full.names = T,recursive = F)

lascarfilepath_caa <- "../Data/CAA Data/LASCAR Files"
file_list_lascar_caa <- list.files(lascarfilepath_caa, pattern='.txt|.TXT|.csv', full.names = T,recursive = F)

beaconfilepath <- "../Data/Data from the team/Beacon Logger"
file_list_beacon <- list.files(beaconfilepath, pattern='.csv|.CSV', full.names = T,recursive = T)


#Load data
postplacement <- readRDS("Processed Data/postplacement.rds")
mobenzi_rapid <- readRDS("Processed Data/mobenzi_rapid.rds")
equipment_IDs <- readRDS("Processed Data/equipment_IDs.rds")
lascar_cali_coefs <- readRDS("Processed Data/lascar_calibration_coefs.rds")
CO_calibrated_timeseries <- readRDS("Processed Data/CO_calibrated_timeseries.rds")
pats_data_timeseries <- readRDS("Processed Data/pats_data_timeseries.rds")
tsi_timeseries <- as.data.table(readRDS("Processed Data/tsi_timeseries.rds"))
beacon_logger_data<- readRDS("Processed Data/beacon_logger_data.rds")
upasmeta<- readRDS("Processed Data/upasmeta.rds")
beacon_meta_qaqc<-readRDS("Processed Data/beacon_meta_qaqc.rds")
lascar_meta<-readRDS("Processed Data/lascar_meta.rds")
tsi_meta_qaqc<-readRDS("Processed Data/tsi_meta_qaqc.rds")
pats_meta_qaqc<-readRDS("Processed Data/pats_meta_qaqc.rds")

#Import preplacement survey and merge with SES results from rapid survey, and with in-depth survey results
#Calculate SES index with rapid survey data
predicted_ses = ses_function(mobenzi_rapid)
saveRDS(predicted_ses,"Processed Data/predicted_ses.rds")
preplacement <- as.data.table(readRDS("Processed Data/preplacement.rds"))
preplacement <- merge(predicted_ses,preplacement,by="HHID",all.y = TRUE)
preplacement <- preplacement[!duplicated(preplacement[5:30]),]
mobenzi_indepth <- readRDS("Processed Data/mobenzi_indepth.rds")
# preplacementtest <- merge(preplacement[,c(1,2,10:12)],mobenzi_indepth[,c(1:5,363)],by="HHID",all.y = FALSE)
saveRDS(preplacement,"Processed Data/ses_preplacement.rds")

# beacon_logger_raw <- readRDS("Processed Data/Beacon_RawData.rds")
all_merged <- readRDS("Processed Data/all_merged.rds")

# Excel metadata import
gravimetric_path <- "../Data/Data from the team/Gravimetric/UNOPS E2E_v2.xlsx"
gravimetric_data <- grav_import_fun(gravimetric_path)

gravimetric_ecm_path <- "~/Dropbox/UNOPS emissions exposure/Data/CAA Data/EldoretGravimetricData.csv"
gravimetric_ecm_data <- grav_ECM_import_fun(gravimetric_ecm_path) %>% 
  dplyr::filter(sampletype == 'Kitchen')

ecm_data <- readRDS("../Data/analysis-20200421/ecm_data.RDS") %>% 
  dplyr::mutate(HHID = pm_hhid) %>%
  dplyr::filter(pm_monitor_type %like% 'Kitchen') %>%
  dplyr::filter(HHID %in% preplacement$HHID)
  

#Merge grav data with ecm data so we can make sure the HHIDs merge correctly (using numeric HHIDs and datetime).  Then will use the grav to perform the corrections.
# ecm_data <- merge(ecm_data,gravimetric_ecm_data,by.x = c('pm_hhid',by.y = 'HHID')
# 
# asdf<-ecm_data %>%
#   group_by(pm_filter_id) %>%
#   summarise_each(funs( min(., na.rm = TRUE), max(., na.rm = TRUE),mean(., na.rm = TRUE))) %>%
#   mutate(time_elapsed = time_chunk_max-time_chunk_min)
# setdiff(sort(unique(ecm_data$pm_hhid)),sort(gravimetric_ecm_data$HHID))

# dot_data <- readRDS("../Data/analysis-20200421/dot_data.RDS")
ecm_meta_data <- readRDS("Processed Data/ecm_meta_data.rds")
ecm_dot_data <- readRDS("../Data/analysis-20200421/ecm_dot_data.RDS") %>%
  dplyr::mutate(stove_type =
                  case_when(stove_type == 'other' ~ 'stove_type_other',
                            stove_type == 'tsf' ~ 'traditional_non_manufactured',
                            TRUE ~ as.character(stove_type)),
                HHID = pm_hhid,
                datetime = time_chunk,
                sampletype = pm_monitor_type,
                qc = "good") %>%
  # dplyr::filter(HHID != 'KE511-KE06' & HHID != 'KE508-KE12') %>%
  dplyr::filter(HHID %in% preplacement$HHID) %>%
  dplyr::filter(!(HHID == 'KE033-KE03' & sampletype == 'Kitchen')) %>%  #Bad data file.
  dplyr::group_by(HHID,Date) %>%
  dplyr::mutate(samplestart = min(datetime[sampletype=='Cook'])) %>%
  dplyr::mutate(sampleend = max(datetime[sampletype=='Cook'])) %>%
  dplyr::mutate(sampleend = case_when(
    difftime(sampleend,samplestart,'days')>1 ~samplestart + 86400,
    TRUE ~ sampleend)) %>%
  dplyr::filter((datetime > min(samplestart,na.rm=TRUE) & datetime < max(sampleend,na.rm=TRUE))) %>%
  dplyr::select(-other_people_use,-meter_name,-meter_id,-notes,-creator_username,-pm_monitor_type,
                -unops,-stove_type_other,-mission_id,-pm_hhid,-time_chunk,-pm_monitor_id,-pm_filter_id,-campaign) %>%
  dplyr::mutate(pm_hhid_numeric = case_when(HHID == 'KE001-KE08' ~ 18,
                          TRUE ~ pm_hhid_numeric)) %>%
  as.data.table() 
# setdiff(unique(preplacement$HHID),unique(ecm_dot_data$HHID))


#Import emissions Excel database 
meta_emissions <- emissions_import_fun(path_emissions,sheetname='Data',local_tz); assign("meta","meta_emissions", envir=.GlobalEnv)
saveRDS(meta_emissions,"Processed Data/meta_emissions.RDS")
meta_emissions<- readRDS("Processed Data/meta_emissions.RDS")

metadata_ambient <- ambient_import_fun(path_other,sheetname='Ambient Sampling')
ambient_data = readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/ambient_data.rds")



