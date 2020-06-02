rm(list = ls()) #clear the environment
## Analyze UNOPS E2E data
# Import data, perform QAQC check.  If email == 1, send an email out.
#Set directory with this file as the working directory.


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
  
  
  # PATS+ data - currently using default calibrations.  Add normalizations as available (Excel, after getting filter weights)
  #Todo: add corrections for a few PATS files.  Truncate based on mobenzi data.
  pats_meta_qaqc <- ldply(setdiff(file_list_pats,processed_filelist), pats_qa_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement)
  pats_data_timeseries <-as.data.table(ldply(setdiff(file_list_pats,processed_filelist), pats_import_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement))
  
  
  # Lascar data
  lascar_meta <- ldply(setdiff(c(file_list_lascar,file_list_pats),processed_filelist), 
                       lascar_qa_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement)  %>% sampletype_fix_function()
  # Time series Lascar - get full time series of calibrated data, with metadata.
  #Need to add truncation function.
  CO_calibrated_timeseries <- as.data.table(ldply(setdiff(c(file_list_lascar,file_list_pats),processed_filelist), 
                                                  lascar_cali_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement)  %>% sampletype_fix_function())
  CO_calibrated_timeseries <- CO_calibrated_timeseries[!sampletype %like% c('NA|TESTP2|NA2|X2'),][!is.na(sampletype),]
  CO_calibrated_timeseries[, sampletype := dt_case_when(sampletype == 'C2' ~ 'Cook Dup',
                                                        sampletype =='12' ~ '1m Dup',
                                                        sampletype == '22' ~ '2m Dup',
                                                        sampletype =='L22' ~ 'Living Room Dup',
                                                        sampletype == 'K22' ~ 'Kitchen Dup',           
                                                        TRUE ~ sampletype)]
  
  # Beacon data
  beacon_logger_raw <- readRDS("Processed Data/Beacon_RawData.rds")
  beacon_meta_qaqc <- ldply(setdiff(file_list_beacon,processed_filelist), beacon_qa_fun, .progress = 'text',timezone="UTC") %>% sampletype_fix_function()
  # Import Beacon timeseries, with metadata 
  beacon_time_corrections=data.frame(filename=c("2019-09-19_KE152-KE004-L_1296","2019-09-19_KE152-KE004-L_129asdfsdf"),
                                     newstartdatetime=c(ymd_hms("2019-09-18T11:44:52GMT",tz="UTC"),ymd_hms("2019-09-18T11:00:52GMT",tz="UTC")))
  # beacon_logger_data <-rbind(beacon_logger_data,as.data.table(ldply(setdiff(file_list_beacon,processed_filelist), beacon_import_fun,.progress = 'text',tz="UTC",preplacement,beacon_time_corrections)))
  beacon_logger_raw <-as.data.table(ldply(file_list_beacon, beacon_import_fun,.progress = 'text',
                                          timezone="UTC",preplacement=preplacement,beacon_time_corrections))  %>% sampletype_fix_function()
  attributes(beacon_logger_raw$datetime)$tzone <- local_tz
  
  # Build Beacon location time series 
  beacon_logger_data <- rbindlist(lapply(mobenzilist, beacon_deployment_fun,equipment_IDs,beacon_logger_raw,
                                         CO_calibrated_timeseries,pats_data_timeseries))  %>% sampletype_fix_function_beacon()
  
  #Calculate SES index with rapid survey data
  predicted_ses = ses_function(mobenzi_rapid)
  saveRDS(predicted_ses,"Processed Data/predicted_ses.rds")
  
  
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
  saveRDS(ecm_meta_data,"Processed Data/ecm_meta_data.rds")
  saveRDS(pats_data_timeseries,"Processed Data/pats_data_timeseries.rds")
  saveRDS(CO_calibrated_timeseries,"Processed Data/CO_calibrated_timeseries.rds")
  saveRDS(upasmeta,"Processed Data/upasmeta.rds")
  saveRDS(lascar_meta,"Processed Data/lascar_meta.rds")
  saveRDS(tsi_meta_qaqc,"Processed Data/tsi_meta_qaqc.rds")
  saveRDS(pats_meta_qaqc,"Processed Data/pats_meta_qaqc.rds")
  saveRDS(beacon_meta_qaqc,"Processed Data/beacon_meta_qaqc.rds")
  saveRDS(beacon_logger_raw,"Processed Data/Beacon_RawData.rds")
  saveRDS(beacon_logger_data,"Processed Data/beacon_logger_data.rds")
  rm(beacon_logger_raw)
  
  # Email out summaries
  if (email==1){emailgroup(todays_date)}
}
# writeLines(c(file_list_beacon,file_list_tsi,file_list_upas,file_list_lascar,file_list_caa), con="Processed Data/processed_filelist.R")




#Create a wide merged dataset (ECM, Dots, TSI, Lascar, PATS+, Beacon Localization, ECM+Beacon exposure estimate
#PATS+Beacon exposure estimate, Lascar+Beacon exposure estimate)
all_merged_list <- all_merge_fun(preplacement,beacon_logger_data,
                                 CO_calibrated_timeseries,tsi_timeseries,pats_data_timeseries,ecm_dot_data)
all_merged <- as.data.table(all_merged_list[1])
saveRDS(all_merged,"Processed Data/all_merged.rds")
all_merged_summary <- distinct(as.data.table(all_merged_list[2]))

for(i in 1:dim(preplacement)[1]){
  plot_deployment(preplacement[i,],beacon_logger_data,
                  pats_data_timeseries,CO_calibrated_timeseries,tsi_timeseries,ecm_dot_data)
}

uniqueHHIDs <- unique(all_merged$HHID)
for(i in 1:length(unique(all_merged$HHID))[1]){
  plot_deployment_merged(all_merged[HHID == uniqueHHIDs[i]])
}


#Indirect exposure estimates
model_indirect_exposure(all_merged_summary,all_merged,preplacement,meta_emissions)


#Ambient data
ambient_analysis(CO_calibrated_timeseries,pats_data_timeseries,upasmeta,gravimetric_data) #Try to get ambient met data from Matt or others?

#Plot kitchen volume distributions
kitchen_volume_plot <- box_plot(meta_emissions, y_var = "roomvolume", fill_var = "qc", x_var = "stovetype", y_units = "m^3",title = "kitchen volume")
ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/kitchen_volume_plot.png",plot=last_plot(),dpi=200,device=NULL)

#Plot ventilation rate distributions

#Plot event duration distributions
event_duration_plot <- box_plot(meta_emissions, y_var = "eventduration", fill_var = "qc", x_var = "stovetype", y_units = "Minutes",title = "event duration")
ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/event_duration_plot.png",plot=last_plot(),dpi=200,device=NULL)

#Plot exposures
scatter_ecm_lpgpercent <- timeseries_plot(ecm_meta_data %>% filter(qc == 'good') 
                                          ,y_var = "`PM µgm-3`", facet_var = "pm_location", x_var = 'lpg_percent',size_var = 'non_lpg_cooking') 
ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/scatter_ecm_lpgpercent.png",plot=last_plot(),dpi=200,device=NULL)


dist_ecm_lpgpercent <- timeseries_plot(ecm_meta_data %>% filter(qc == 'good') 
                                       , y_var = "`PM µgm-3`", facet_var = "pm_location", x_var = 'lpg_percent',size_var = 'non_lpg_cooking') 
ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/dist_ecm_lpgpercent.png",plot=last_plot(),dpi=200,device=NULL)


boxplot_ecm_kitchen_cook <- box_plot_facet(ecm_meta_data %>% filter(qc == 'good') 
                                           , y_var = "`PM µgm-3`", facet_var = "pm_location", x_var = "primary_stove", y_units = "µgm-3",title = "ECM PM2.5 concentration" )
ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/boxplot_ecm_kitchen_cook.png",plot=last_plot(),dpi=200,device=NULL)

scatter_ecm_kitchen_cook <- timeseries_plot_simple(pivot_wider(ecm_meta_data,
                                                               names_from = pm_location,
                                                               values_from = c(`PM µgm-3`,datetime_start,datetime_end,
                                                                               sampling_duration,samplerate_minutes,lpg_cooking,non_lpg_cooking,lpg_percent)) %>%
                                                     rename(`Cook's PM µgm-3` = `PM µgm-3_Cook`,
                                                            `Kitchen PM µgm-3` = `PM µgm-3_Kitchen`),
                                                   y_var = "`Cook's PM µgm-3`", x_var = "`Kitchen PM µgm-3`")
ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/scatter_ecm_kitchen_cook.png",plot=last_plot(),dpi=200,device=NULL)

ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/scatter_ecm_lpgpercent.png",plot=last_plot(),dpi=200,device=NULL)


#Plot kitchen concentration distributions
HAP_CO_plot <- box_plot(lascar_meta  %>% as.data.frame() %>% left_join(meta_emissions,by="HHID")
                        , y_var = "eventduration", fill_var = "stovetype", x_var = "sampletype", y_units = "ppm",title = "CO concentration" )
ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/HAP_CO_plot.png",plot=last_plot(),dpi=200,device=NULL)

HAP_PM_plot <- box_plot(pats_meta_qaqc %>% sampletype_fix_function() %>% as.data.frame() %>% left_join(meta_emissions,by="HHID")
                        , y_var = "eventduration", fill_var = "stovetype", x_var = "sampletype", y_units = "µgm-3",title = "PATS+ PM concentration" )
ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/HAP_PM_plot.png",plot=last_plot(),dpi=200,device=NULL)


#######Modeling



######QAQC/bonus analyses
#Measured cooking with the SUMs, compared with the logged cooking time during emissions

#Compare PATS with ECM results

# CO_duplicate_analysis(CO_calibrated_timeseries)

#Get the threshold times and room IDs, and compare with beacon localization results
walkthrough_results <- beacon_walkthrough(beacon_logger_data,preplacement)

#Analyze intensive household data.

#Change the maxdatetime if there is extended data.
if(difftime(maxdatetimeCO$datetime,mindatetime)>2){
  maxdatetime = maxdatetime + 60*60*72
}



rm(list = ls())

