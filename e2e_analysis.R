rm(list = ls()) #clear the environment
graphics.off()
## Analyze UNOPS E2E data
# Import data, perform QAQC check.  If email == 1, send an email out.
#Set directory with this file as the working directory.
#Changing the numeric household ID for KE001-KE08 from 1 to 18, due to conflicts with other deployments.  Happens within each instrument function


#Keys: HHID, sampletype, location, datetime, HHIDdate
dir <- '~/Dropbox/UNOPS emissions exposure/E2E Data Analysis'
setwd(dir)
source('r_scripts/load.R')
source('r_scripts/load_data_functions_paths.R')
email = 0 #Set to 1 to send out summary qaqc email, else 0
averaging_window = 1 #Minutes.  Use 1 or 5 generally.
processed_filelist <- "c" #readLines('Processed Data/processed_filelist.csv') #uncomment to speed up, once fully debugged.
import = 0 #Set to 1 to re-import data.  Otherwise, saved versions are loaded from load_data_functions_paths.R
mobenzilist = lapply(as.list(1:dim(preplacement)[1]), function(x) preplacement[x[1],])
todays_date <- gsub("-", "", as.character(Sys.Date()))

if (import==1){
  
  # TSI Data.  TSI data needs to be manually prepared by only keeping the data from the test of interest.  Multiple tests in the same file will break it.
  #If there is an error with AER, check the emissions databased in the decaystarttime and decayendtime fields - likely an empty row or NA value that is breaking it.
  tsi_meta_qaqc <- ldply(setdiff(file_list_tsi,processed_filelist), tsi_qa_fun, .progress = 'text',meta_emissions=meta_emissions,local_tz) 
  tsi_timeseries <- ldply(file_list_tsi, tsi_meta_data_fun, .progress = 'text',local_tz=local_tz,meta_emissions=meta_emissions) 
    
  
  # ECM/MicroPEM data, join with the PATS+ data.  Includes compliance check and basic QAQC
  #This needs to go before other instruments, because once we get it we will use it as the reference start and stop time.  
  # the function 'update_preplacement' needs to be finished, updated with the ECM's start and stop times.
  ecm_meta_data <- ecm_process_metadata(ecm_dot_data)
  
  # UPAS data import and check
  upasmeta <- as.data.frame(ldply(setdiff(file_list_upas,processed_filelist), UPAS_qa_fun, local_tz,.progress = 'text'))
  
  
  # PATS+ data - uses ECM grav correction for any collocated samples. 
  #Todo: add corrections for a few PATS files.  Truncate based on mobenzi data.
  pats_meta_qaqc <- ldply(setdiff(file_list_pats,processed_filelist), pats_qa_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement)
  pats_data_timeseries <-as.data.table(ldply(setdiff(file_list_pats,processed_filelist), pats_import_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement))
                                         

  # Lascar data
  lascar_meta <- ldply(setdiff(c(file_list_lascar,file_list_pats),processed_filelist), 
                       lascar_qa_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement)  %>% sampletype_fix_function()
  # Time series Lascar - get full time series of calibrated data, with metadata.
  # Truncation is employed with the tagging approach - any non-tagged points are not from during the deployments
  CO_calibrated_timeseries <- as.data.table(ldply(setdiff(c(file_list_lascar,file_list_pats),processed_filelist), 
                                                  lascar_cali_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement)  %>%
                                              sampletype_fix_function())
  CO_calibrated_timeseries <- CO_calibrated_timeseries[!sampletype %like% c('NA|TESTP2|NA2|X2'),][!is.na(sampletype),]
  
  
  # Beacon data
  # beacon_logger_raw <- readRDS("Processed Data/Beacon_RawData.rds")
  beacon_meta_qaqc <- ldply(setdiff(file_list_beacon,processed_filelist), beacon_qa_fun, .progress = 'text',timezone="UTC") %>% sampletype_fix_function()
  # Import Beacon timeseries, with metadata 
  beacon_time_corrections=data.frame(filename=c("2019-09-19_KE152-KE004-L_1296","2019-09-19_KE152-KE004-L_129asdfsdf"),
                                     newstartdatetime=c(ymd_hms("2019-09-18T11:44:52GMT",tz="UTC"),ymd_hms("2019-09-18T11:00:52GMT",tz="UTC")))
  # beacon_logger_data <-rbind(beacon_logger_data,as.data.table(ldply(setdiff(file_list_beacon,processed_filelist), beacon_import_fun,.progress = 'text',tz="UTC",preplacement,beacon_time_corrections)))
  beacon_logger_raw <-as.data.table(ldply(file_list_beacon, beacon_import_fun,.progress = 'text',
                                          timezone="UTC",preplacement=preplacement,beacon_time_corrections))  %>% sampletype_fix_function()
  
  # Build Beacon location time series 
  beacon_logger_data <- rbindlist(lapply(mobenzilist, beacon_deployment_fun,equipment_IDs,beacon_logger_raw,
                                         CO_calibrated_timeseries,pats_data_timeseries))  %>% 
    sampletype_fix_function_beacon() 
  
  
  #Check filenames of other instruments:
  # filename_otherinstruments <- rbindlist(lapply(c(file_list_PATS), FUN=parse_filename_fun))
  
  
  # Use Mobenzi to figure out what instruments were in each deployments, compare with file names.
  #Add
  deployment_check = ldply(mobenzilist, deployment_check_fun, .progress = 'text',equipment_IDs,tz,local_tz,meta_emissions,beacon_meta_qaqc,tsi_meta_qaqc,lascar_meta,upasmeta)
  
  
  list_of_datasets <- list("Beacons" = beacon_meta_qaqc, "Lascar" = lascar_meta,
                           "UPAS"=upasmeta,"TSI"=tsi_meta_qaqc,"PATS"=pats_meta_qaqc,
                           # "Other Filenames"=filename_otherinstruments,
                           "Deployment Summary"=deployment_check)
  
  #Save processed data sets
  write.xlsx(list_of_datasets, file = paste0("QA Reports/QA report", "_", todays_date, ".xlsx"))
  saveRDS(tsi_timeseries,"Processed Data/tsi_timeseries.rds")
  saveRDS(pats_data_timeseries,"Processed Data/pats_data_timeseries.rds")
  saveRDS(pats_meta_qaqc,"Processed Data/pats_meta_qaqc.rds")
  saveRDS(CO_calibrated_timeseries,"Processed Data/CO_calibrated_timeseries.rds")
  saveRDS(lascar_meta,"Processed Data/lascar_meta.rds")
  saveRDS(tsi_meta_qaqc,"Processed Data/tsi_meta_qaqc.rds")
  saveRDS(beacon_meta_qaqc,"Processed Data/beacon_meta_qaqc.rds")
  saveRDS(beacon_logger_raw,"Processed Data/Beacon_RawData.rds")
  saveRDS(beacon_logger_data,"Processed Data/beacon_logger_data.rds")
  saveRDS(upasmeta,"Processed Data/upasmeta.rds")
  saveRDS(ecm_meta_data,"Processed Data/ecm_meta_data.rds")
  
  rm(beacon_logger_raw)
  
  # Email out summaries
  if (email==1){emailgroup(todays_date)}
}
# writeLines(c(file_list_beacon,file_list_tsi,file_list_upas,file_list_lascar,file_list_caa), con="Processed Data/processed_filelist.R")


#Create a wide merged dataset (ECM, Dots, TSI, Lascar, PATS+, Beacon Localization, ECM+Beacon exposure estimate
#PATS+Beacon exposure estimate, Lascar+Beacon exposure estimate)
#Also performs ECM/PATS calibration and subs in PATS for ECM if it is not available.
all_merged_list <- all_merge_fun(preplacement,beacon_logger_data,
                                 CO_calibrated_timeseries,tsi_timeseries,pats_data_timeseries,ecm_dot_data)
all_merged <- as.data.table(all_merged_list[1])
saveRDS(all_merged,"Processed Data/all_merged.rds")
all_merged_summary <- distinct(as.data.table(all_merged_list[2]))
saveRDS(all_merged_summary,"Processed Data/all_merged_summary.rds")
rm(all_merged_list)

#Plot deployments
all_merged %>%
  group_by(HHID,Date) %>%
  do(plot_deployment_merged(.))


#Indirect exposure estimates
model_indirect_exposure(all_merged_summary,all_merged,preplacement,meta_emissions)


#Ambient data
ambient_analysis(CO_calibrated_timeseries,pats_data_timeseries,upasmeta,gravimetric_data) #Try to get ambient met data from Matt or others?



######QAQC/bonus analyses
#Measured cooking with the SUMs, compared with the logged cooking time during emissions


#Get the threshold times and room IDs, and compare with beacon localization results
walkthrough_results <- beacon_walkthrough_function(beacon_logger_data,preplacement)

#Analyze intensive household data.

#Change the maxdatetime if there is extended data.
if(difftime(maxdatetimeCO$datetime,mindatetime)>2){
  maxdatetime = maxdatetime + 60*60*72
}



rm(list = ls())

