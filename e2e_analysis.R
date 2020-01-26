## Analyze UNOPS E2E data
# Import data, perform QAQC check.  If email == 1, send an email out.
#Set directory with this file as the working directory.


#Keys: HHID, sampletype, location, datetime, HHIDdate
source('r_scripts/load.R')
source('r_scripts/load_data_functions_paths.R')

todays_date <- gsub("-", "", as.character(Sys.Date()))
local_tz = "Africa/Nairobi"
processed_filelist <- "c" #readLines('Processed Data/processed_filelist.csv') #uncomment to speed up, once fully debugged.
email = 0 #Set to 1 to send out summary qaqc email, else 0
averaging_window = 1 #Minutes.  Use 1 or 5 generally.


# Excel metadata import
metadata_filter_record <- upas_record_import_fun(path_other, sheetname='UPAS Filter record')
meta_emissions <- emissions_import_fun(path_emissions,sheetname='Ambient Sampling',local_tz)
metadata_ambient <- ambient_import_fun(path_other,sheetname='Ambient Sampling')


# TSI Data.  TSI data needs to be manually prepared by only keeping the data from the test of interest.  Multiple tests in the same file will break it.
#If there is an error with AER, check the emissions databased in the decaystarttime and decayendtime fields - likely an empty row or NA value that is breaking it.
tsi_meta_qaqc <- ldply(setdiff(file_list_tsi,processed_filelist), tsi_qa_fun, .progress = 'text',meta_emissions=meta_emissions,local_tz) 
tsi_timeseries <- ldply(file_list_tsi, tsi_meta_data_fun, .progress = 'text',local_tz=local_tz)


# ECM/MicroPEM data, join with the PATS+ data.  Includes compliance check and basic QAQC
#This needs to go befor other instruments, because once we get it we will use it as the reference start and stop time.  
# the function 'update_preplacement' needs to be finished, updated with the ECM's start and stop times.
ecm_data_timeseries <-ldply(file_list_ECM, ecm_import_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement)



# UPAS data import and check
upasmeta <- as.data.frame(ldply(setdiff(file_list_upas,processed_filelist), UPAS_qa_fun, local_tz,.progress = 'text'))


# PATS+ data - currently using default calibrations.  Add normalizations as available (Excel, after getting filter weights)
#Todo: add corrections for a few PATS files.  Truncate based on mobenzi data.
pats_meta_qaqc <- ldply(file_list_pats, pats_qa_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement)
pats_data_timeseries <-as.data.table(ldply(file_list_pats, pats_import_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement))


# Lascar data
lascar_meta <- ldply(setdiff(c(file_list_lascar,file_list_lascar_caa,file_list_pats),processed_filelist), lascar_qa_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement)  
# Time series Lascar - get full time series of calibrated data, with metadata.
#Need to add truncation function.
# CO_calibrated_timeseries <- rbind(CO_calibrated_timeseries,ldply(setdiff(c(file_list_lascar,file_list_lascar_caa),processed_filelist), lascar_cali_fun, .progress = 'text',tz=local_tz))
CO_calibrated_timeseries <- as.data.table(ldply(c(file_list_lascar,file_list_lascar_caa,file_list_pats), lascar_cali_fun, .progress = 'text',local_tz=local_tz,preplacement=preplacement))


# Beacon data
beacon_meta_qaqc <- ldply(setdiff(file_list_beacon,processed_filelist), beacon_qa_fun, .progress = 'text',timezone="UTC")#This is correct as UTC because the data was collected in UTC.
# Import Beacon timeseries, with metadata
beacon_time_corrections=data.frame(filename=c("2019-09-19_KE152-KE004-L_1296","2019-09-19_KE152-KE004-L_129asdfsdf"),
                                   newstartdatetime=c(ymd_hms("2019-09-18T11:44:52GMT",tz="UTC"),ymd_hms("2019-09-18T11:00:52GMT",tz="UTC")))
# beacon_data_timeseries <-rbind(beacon_data_timeseries,as.data.table(ldply(setdiff(file_list_beacon,processed_filelist), beacon_import_fun,.progress = 'text',tz="UTC",preplacement,beacon_time_corrections)))
beacon_data_timeseries <-as.data.table(ldply(file_list_beacon, beacon_import_fun,.progress = 'text',TimeZone="UTC",preplacement=preplacement,beacon_time_corrections))


# * Todo: Get Stove usage data integrated into analysis stream.
# * Do basic usage analysis using the SUMs code
# * Integrate events as time series flags.  Name variables based on possible stove types/locations: Indoor Trad/Outdoor Trad/LPG1/LPG2/Jiko/TraditionalCharcoal, etc.


#Check filenames of other instruments:
filename_otherinstruments <- rbindlist(lapply(c(file_list_micropem,file_list_PATS), FUN=parse_filename_fun))


# Use Mobenzi to figure out what instruments were in each deployments, compare with file names.
#Add
deployment_check = ldply(mobenzilist, deployment_check_fun, .progress = 'text',equipment_IDs,tz,local_tz,meta_emissions,beacon_meta_qaqc,tsi_meta_qaqc,lascar_meta,upasmeta)


list_of_datasets <- list("Beacons" = beacon_meta_qaqc, "Lascar" = lascar_meta,
                         "UPAS"=upasmeta,"TSI"=tsi_meta_qaqc,"PATS"=pats_meta_qaqc,
                         "Other Filenames"=filename_otherinstruments,
                         "Deployment Summary"=deployment_check)

#Save processed data sets
write.xlsx(list_of_datasets, file = paste0("QA Reports/QA report", "_", todays_date, ".xlsx"))
saveRDS(tsi_timeseries,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/tsi_timeseries.rds")
saveRDS(pats_data_timeseries,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/pats_data_timeseries.rds")
saveRDS(CO_calibrated_timeseries,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/CO_calibrated_timeseries.rds")
saveRDS(beacon_data_timeseries,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Beacon_RawData.rds")


# Email out summaries
if (email==1){emailgroup(todays_date)}

# writeLines(c(file_list_beacon,file_list_tsi,file_list_upas,file_list_lascar,file_list_caa), con="Processed Data/processed_filelist.R")


  

ambient_data <- ambient_timeseries(CO_calibrated_timeseries,pats_data_timeseries) #Try to get ambient met data from Matt or others?
saveRDS(ambient_data,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/ambient_data.rds")

#Perform analyses/modeling #ldply - for each deployment, run the model
# Build Beacon location time series with exposure estimates
beacon_logger_COmerged <- ldply(mobenzilist, beacon_deployment_fun, .progress = 'text',equipment_IDs,local_tz,beacon_data_timeseries, CO_calibrated_timeseries,pats_data_timeseries)
saveRDS(beacon_logger_COmerged,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/beacon_logger_COmerged.rds")


# CO_duplicate_analysis(CO_calibrated_timeseries)

#Get the threshold times and room IDs, and compare with beacon localization results
# beacon_walkthrough()

#Plot data
# plot_ambient <- timeseries_plot(dplyr::filter(ambient_data,variable=="CO_ppm" | variable=="PM_estimate")
                                # , y_var="value", x_var="datetime", facet_var="variable",color_var="qc", marker_shape="qc")


#Deployment plots
# ldply(mobenzilist, deployment_plots, .progress = 'text',equipment_IDs,tz,local_tz,meta_emissions,beacon_meta_qaqc,tsi_meta_qaqc,lascar_meta,upasmeta)

#Analyze intensive household data.


#Plot model results

# #clear ws and quit
rm(list = ls())
# quit(save = 'no')
