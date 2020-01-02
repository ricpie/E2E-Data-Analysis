## Analyze UNOPS E2E data
source('r_scripts/load.R')

processed_filelist <- "c" #readLines('Processed Data/processed_filelist.csv') #uncomment to speed up, once fully debugged.
lascar_cali_import()
mobenzi <- mobenzi_import_fun() # Import Mobenzi data


# Excel metadata import
path_other <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/Excel databases/Other databases.xlsx"
metadata_ambient <- ambient_import_fun(path_other,sheetname='Ambient Sampling')
metadata_filter_record <- upas_record_import_fun(path_other, sheetname='UPAS Filter record')
path_emissions <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/Excel databases/2019 Kenya Emissions database.xlsx"
meta_emissions <- emissions_import_fun(path_emissions,sheetname='Ambient Sampling')


# TSI Data.  TSI data needs to be manually prepared by only keeping the data from the test of interest.  Multiple tests in the same file will break it.
# tsifilepath <- "/Users/ricardopiedrahita/Dropbox/CA(Kenya)/UNOPs/Data from the team/TSI"
tsifilepath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Cleaned TSI Data" #the corrected files should be placed here.
file_list_tsi <- list.files(tsifilepath, pattern='.xls|.XLS', full.names = T,recursive = T)
tsi_meta <- ldply(setdiff(file_list_tsi,processed_filelist), tsi_qa_fun, .progress = 'text',meta_emissions=meta_emissions,local_tz=local_tz) 
#Time series TSI
tsi_timeseries <- rbind(tsi_timeseries, ldply(setdiff(file_list_tsi,processed_filelist), tsi_metadata_fun, .progress = 'text',local_tz=local_tz))
saveRDS(tsi_timeseries,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/tsi_timeseries.R")


# UPAS data
datafilepath <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/UPAS" #For real
file_list_upas <- list.files(datafilepath, pattern='.txt|.TXT', full.names = T,recursive = F)
upasmeta <- as.data.frame(ldply(setdiff(file_list_upas,processed_filelist), UPAS_qa_fun, local_tz,.progress = 'text'))


# Lascar data
lascarfilepath <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/Lascar" #For real
lascarfilepath_caa <- "~/Dropbox/CA(Kenya)/Clean Air Africa/LASCAR Files"
file_list_lascar <- list.files(lascarfilepath, pattern='.txt|.TXT', full.names = T,recursive = T)
file_list_lascar_caa <- list.files(lascarfilepath_caa, pattern='.txt|.TXT', full.names = T,recursive = F)
lascar_meta <- ldply(setdiff(c(file_list_lascar,file_list_lascar_caa),processed_filelist), lascar_qa_fun, .progress = 'text',tz=local_tz)  

# Time series Lascar
lascar_calibrated_timeseries <- rbind(lascar_calibrated_timeseries,ldply(setdiff(c(file_list_lascar,file_list_lascar_caa),processed_filelist), lascar_cali_fun, .progress = 'text',tz=local_tz))
saveRDS(lascar_calibrated_timeseries,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Calibrated Lascar.R")


# PATS+ data - currently using default calibrations.  Add normalizations as available (Excel, after getting filter weights)
patsfilepath <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/PATS+"
file_list_pats <- list.files(patsfilepath, pattern='.csv|.CSV', full.names = T,recursive = T)
pats_meta_qaqc <- ldply(file_list_pats, pats_qa_fun, .progress = 'text',tz="UTC")
pats_data_timeseries <-as.data.table(ldply(file_list_pats, pats_import_fun, .progress = 'text',tz="UTC",mobenzi$preplacement))
saveRDS(pats_data_timeseries,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/pats_data_timeseries.R")
# Import ECM/Micropem data, and join with the PATS+ data

# Beacon data
beaconfilepath <- "~/Dropbox/CA(Kenya)/UNOPs/Data from the team/Beacon Logger"
file_list_beacon <- list.files(beaconfilepath, pattern='.csv|.CSV', full.names = T,recursive = T)
beacon_meta_qaqc <- ldply(setdiff(file_list_beacon,processed_filelist), beacon_qa_fun, .progress = 'text',tz="UTC")#This is correct as UTC because the data was collected in UTC.

# Import Beacon timeseries, with metadata
beacon_time_corrections=data.frame(filename=c("2019-09-19_KE152-KE004-L_1296","2019-09-19_KE152-KE004-L_129asdfsdf"),
                                   newstartdatetime=c(ymd_hms("2019-09-18T11:44:52GMT",tz="UTC"),ymd_hms("2019-09-18T11:00:52GMT",tz="UTC")))
beacon_data_timeseries <-rbind(beacon_data_timeseries,as.data.table(ldply(setdiff(file_list_beacon,processed_filelist), beacon_import_fun,
                                                                          .progress = 'text',tz="UTC",mobenzi$preplacement,beacon_time_corrections)))
saveRDS(beacon_data_timeseries,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Beacon_RawData.R")

# Build Beacon location time series
# Use Mobenzi to figure out what instruments were in each deployments, plot stuff.
mobenzilist <- lapply(seq_len(nrow(mobenzi$preplacement)), function(i) mobenzi$preplacement[i,])
beacon_localization <- ldply(mobenzilist, beacon_deployment_fun, .progress = 'text',equipment_IDs,tz,local_tz,beacon_data_timeseries,
                             lascar_calibrated_timeseries,pats_data_timeseries) 
saveRDS(beacon_localization,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Beacon_Locations.R")

# * []Get Stove usage data integrated into analysis stream.
# * Do basic usage analysis using the SUMs code
# * Integrate events as time series flags.  Name variables based on possible stove types/locations: Indoor Trad/Outdoor Trad/LPG1/LPG2/Jiko/TraditionalCharcoal, etc.

#Check filenames of other instruments:
file_list_micropem <- list.files("~/Dropbox/CA(Kenya)/Clean Air Africa/MicroPEM and ECM Raw Files", pattern='.csv|.CSV', full.names = T,recursive = T)
file_list_PATS <- list.files("~/Dropbox/CA(Kenya)/UNOPs/Data from the team/PATS+", pattern='.csv|.CSV', full.names = T,recursive = T)
filename_otherinstruments <- rbindlist(lapply(c(file_list_micropem,file_list_PATS), FUN=parse_filename_fun))


# Build deployment checklist.
deployment_check = ldply(mobenzilist, deployment_check_fun, .progress = 'text',equipment_IDs,tz,local_tz,meta_emissions,beacon_meta_qaqc,tsi_meta,lascar_meta,upasmeta)


list_of_datasets <- list("Beacons" = beacon_meta_qaqc, "Lascar" = lascar_meta,
                         "UPAS"=upasmeta,"TSI"=tsi_meta,"PATS"=pats_meta_qaqc,
                         "Other Filenames"=filename_otherinstruments,
                         "Deployment Summary"=deployment_check)
write.xlsx(list_of_datasets, file = paste0("QA Reports/QA report", "_", todays_date, ".xlsx"))

# Email out summaries
sender <- "beaconnih@gmail.com"
recipients <- c("rpiedrahita@berkeleyair.com","M.Shupler@liverpool.ac.uk","mrossanese@berkeleyair.com","sdelapena@berkeleyair.com")
send.mail(from = sender,
          to = recipients,
          subject = paste0("UNOPS/CAA QA Report ",as.character(Sys.Date())),
          body = "UNOPS/CAA data report (new format). Please correct filenames as needed, and note any devices that need maintenance. 
          The Deployment Summary worksheet has information from the filenames, Mobenzi, and Excel emissions database.  These should be cross-referenced and disparities investigated!",
          smtp = list(host.name = "smtp.gmail.com", port = 465, 
                      user.name = "beaconnih@gmail.com",            
                      passwd = "CookaBLE99", ssl = TRUE),
          authenticate = TRUE,
          attach.files = c(paste0("QA Reports/QA report", "_", todays_date, ".xlsx")),
          send = TRUE)

# writeLines(c(file_list_beacon,file_list_tsi,file_list_upas,file_list_lascar,file_list_caa), con="Processed Data/processed_filelist.R")


# #clear ws and quit
rm(list = ls())
# quit(save = 'no')