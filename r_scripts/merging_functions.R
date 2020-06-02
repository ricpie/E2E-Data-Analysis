

##################################################
#Merge the time series data sets and calculate exposure estimates
##################################################

all_merge_fun = function(preplacement,beacon_logger_data,
                         CO_calibrated_timeseries,tsi_timeseries,pats_data_timeseries,ecm_dot_data){
  
  #Merge ecm with everything, sequentially
  # ecm_dot_data needs to go wide on dupes and sums
  ecm_dot_data[,(c('other_people_use_n','pm_accel','file','mission_name',
                   'indoors','shared_cooking_area','pm_rh','pm_temp','dot_temperature','qc')) := NULL]  
  
  #This chunk aggregates data from multiple stoves of the same type into a single stove (e.g. two lpg stoves becomes one, TRUE cooking state is kept if either one is true)
  #Uses the max compliance (will be of the cook), available.
  ecm_dot_data <-  dplyr::group_by(ecm_dot_data,HHID,stove_type,datetime,sampletype) %>%
    dplyr::arrange(desc(cooking)) %>%
    dplyr::filter(row_number()==1) %>%
    dplyr::arrange(datetime) %>%
    dplyr::group_by(HHID,datetime) %>%
    dplyr::mutate(pm_compliant = max(pm_compliant)) %>%
    dplyr::ungroup() %>% as.data.table() 
  
  #Take the ecm data wide so we can get a single time series, onto which we can merge other data streams.
  wide_ecm_dot_data <- pivot_wider(ecm_dot_data,
                                   names_from = c(sampletype),
                                   values_from = c(3),
                                   names_prefix = 'PM25') %>%
    pivot_wider(names_from = c(stove_type),
                values_from = c(4),
                names_prefix = 'sums') %>% 
    select(-sumsNA) %>% as.data.table()
  
  
  # lascar data needs to go wide on location# lascar data needs to go wide on location (sampletype)
  # remove a few cols
  CO_calibrated_timeseries <- readRDS("Processed Data/CO_calibrated_timeseries.rds")[qc == 'good']
  
  CO_calibrated_timeseries[,(c('qc','ecm_tags','fullname','basename','datetime_start','qa_date','sampleID','loggerID',
                               'filterID','fieldworkerID','fieldworkernum','samplerate_minutes','sampling_duration_hrs','file','flags')) := NULL]
  CO_calibrated_timeseries[, sampletype := dt_case_when(sampletype == 'Cook Dup' ~ 'Cook',
                                                        sampletype =='1m Dup' ~ '1m',
                                                        sampletype == '2m Dup' ~ '2m',
                                                        sampletype == 'Living Room Dup' ~ 'LivingRoom',
                                                        sampletype == 'Living Room' ~ 'LivingRoom',
                                                        sampletype == 'Ambient Dup' ~ 'Ambient',
                                                        sampletype == 'Kitchen Dup' ~ 'Kitchen',           
                                                        TRUE ~ sampletype)]
  CO_calibrated_timeseries[,measure := "COppm"]
  CO_calibrated_timeseries[, sampletype := paste0("CO_ppm", sampletype)]
  
  
  #Use averages if there are duplicates
  CO_calibrated_timeseries <-  dplyr::group_by(CO_calibrated_timeseries,HHID,sampletype,datetime) %>%
    dplyr::mutate(CO_ppm = mean(CO_ppm)) %>%
    dplyr::filter(row_number()==1) %>%
    dplyr::ungroup() %>% as.data.table() 
  
  
  CO_calibrated_timeseries_hap <- CO_calibrated_timeseries[HHID!='777' & HHID!='888']
  CO_calibrated_timeseries_ambient <- CO_calibrated_timeseries[HHID=='777'
                                                               ][,CO_ppmAmbient:=CO_ppm
                                                                 ][,c('measure','HHID','CO_ppm','sampletype','emission_tags'):=NULL]
  rm(CO_calibrated_timeseries)
  
  wide_co_data <- dcast.data.table(CO_calibrated_timeseries_hap,datetime + HHID ~ sampletype, value.var = c("CO_ppm"))
  
  #Merge house/personal time serires with ambient data (based on date time only, not HHID)
  wide_co_data = merge(wide_co_data,CO_calibrated_timeseries_ambient, by.x = c("datetime"),
                       by.y = c("datetime"), all.x = T, all.y = F,)
  
  
  #Import and prep PATS data
  pats_data_timeseries <- readRDS("Processed Data/pats_data_timeseries.rds")[qc == 'good']
  
  pats_data_timeseries[,c('V_power', 'degC_air','%RH_air','CO_PPM','status','ref_sigDel','low20avg','loggerID',
                          'high320avg','motion','ecm_tags','emission_tags','sampleID','qc') := NULL]
  pats_data_timeseries[, sampletype := dt_case_when(sampletype == 'K2' ~ 'Kitchen',
                                                    sampletype =='K' ~ 'Kitchen',
                                                    sampletype == '2' ~ '2m',
                                                    sampletype == '1' ~ '1m',
                                                    sampletype == 'A' ~ 'Ambient',
                                                    sampletype == 'L' ~ 'LivingRoom',
                                                    sampletype == 'L2' ~ 'LivingRoom',
                                                    TRUE ~ sampletype)]
  pats_data_timeseries[,measure := "pm25_conc"]
  pats_data_timeseries[, sampletype := paste0("PATS_", sampletype)]
  
  #Group data from duplicates into a mean value.
  pats_data_timeseries <-  dplyr::group_by(pats_data_timeseries,HHID,sampletype,datetime) %>%
    dplyr::mutate(pm25_conc = mean(pm25_conc)) %>%
    dplyr::filter(row_number()==1) %>%
    dplyr::ungroup()  %>% as.data.table() 
  
  pats_data_timeseries_hap <- pats_data_timeseries[HHID!='777' & HHID!='888']
  pats_data_timeseries_ambient <- pats_data_timeseries[HHID=='777'
                                                       ][,pm25_concAmbient:=pm25_conc
                                                         ][,c('measure','HHID','pm25_conc','sampletype'):=NULL]
  rm(pats_data_timeseries)
  
  wide_pats_data <- dcast.data.table(pats_data_timeseries_hap,datetime + HHID ~ sampletype, value.var = c("pm25_conc"))
  
  #Merge house/personal time serires with ambient data (based on date time only, not HHID)
  wide_pats_data = merge(wide_pats_data,pats_data_timeseries_ambient, by.x = c("datetime"),
                         by.y = c("datetime"), all.x = T, all.y = F,)
  
  
  
  # Prep beacon data (only exists for cook)
  beacon_logger_data<- readRDS("Processed Data/beacon_logger_data.rds")[qc == 'good']
  beacon_logger_data[,measure := "beacon"]
  beacon_logger_data[,c('location_kitchen','location_livingroom','loggerID_Kitchen','qc','loggerID_LivingRoom','HHIDstr') := NULL]
  
  # Prep emissions, (tsi) 
  tsi_timeseries <- as.data.table(readRDS("Processed Data/tsi_timeseries.rds"))[qc == 'good']
  tsi_timeseries[,measure := "emissions"]
  tsi_timeseries[,(c('Date','Time','sampleID','qc','sampletype','RH','Temp_C')) := NULL]
  
  
  # merge dot, ecm, pats, lascar, tsi data.  preplacement survey not merged here.
  
  all_merged = merge(wide_ecm_dot_data,wide_co_data, by.x = c("datetime","pm_hhid_numeric"),
                     by.y = c("datetime","HHID"), all.x = T, all.y = F)
  all_merged = merge(all_merged,wide_pats_data, by.x = c("datetime","pm_hhid_numeric"),
                     by.y = c("datetime","HHID"), all.x = T, all.y = F)
  all_merged <- merge(all_merged,beacon_logger_data, by.x = c("datetime","pm_hhid_numeric"),
                      by.y = c("datetime","HHID"), all.x = T, all.y = F)
  all_merged = merge(all_merged,tsi_timeseries, by.x = c("datetime","pm_hhid_numeric"),
                     by.y = c("datetime","HHID"), all.x = T, all.y = F)
  
  all_merged<-all_merged[order(rank(pm_hhid_numeric)),][,'NA' := NULL]
  
  
  
  #### Assign exposure based on nearest beacon (use both methods still)
  
  ####First batch uses nearest location
  all_merged[, pm25_conc_beacon_nearest_ecm := dt_case_when(location_nearest == 'Kitchen' ~ PM25Kitchen,
                                                            location_nearest != 'Kitchen' ~ PATS_LivingRoom,
                                                            !is.na(PATS_Ambient) ~ PATS_Ambient,
                                                            TRUE ~ mean(PATS_Ambient,na.rm = TRUE))]
  all_merged[, pm25_conc_beacon_pats := dt_case_when(location_nearest == 'Kitchen' ~ PATS_Kitchen,
                                                     location_nearest != 'Kitchen' ~ PATS_LivingRoom,
                                                     !is.na(PATS_Ambient) ~ PATS_Ambient,
                                                     TRUE ~ mean(PATS_Ambient,na.rm = TRUE))]
  
  #Estimate uses kitchen and living room co, and ambient if needed.
  all_merged[, co_estimate_beacon_nearest := dt_case_when(location_nearest == 'Kitchen' ~ CO_ppmKitchen,
                                                          location_nearest != 'Kitchen' ~ CO_ppmLivingRoom,
                                                          !is.na(CO_ppmAmbient) ~ CO_ppmAmbient,
                                                          TRUE ~ mean(CO_ppmAmbient,na.rm = TRUE))]  
  
  ####This batch uses kitchen if we have signal greater than x RSSI in the kitchen, otherwise, living room, otherwise ambient
  #Estimate uses kitchen ecm data and living room pats data, and ambient pats if needed.
  all_merged[, pm25_conc_beacon_nearestthreshold_ecm := dt_case_when(location_kitchen_threshold == 'Kitchen' ~ PM25Kitchen,
                                                                     location_kitchen_threshold != 'Kitchen' ~ PATS_LivingRoom,
                                                                     !is.na(PATS_Ambient) ~ PATS_Ambient,
                                                                     TRUE ~ mean(PATS_Ambient,na.rm = TRUE))]
  

  all_merged[, pm25_conc_beacon_nearestthreshold_ecm80 := dt_case_when(location_kitchen_threshold80 == 'Kitchen' ~ PM25Kitchen,
                                                                       location_kitchen_threshold80 != 'Kitchen' ~ PATS_LivingRoom,
                                                                       !is.na(PATS_Ambient) ~ PATS_Ambient,
                                                                       TRUE ~ mean(PATS_Ambient,na.rm = TRUE))]
  
  #Estimate uses kitchen and living room PATS, and ambient if needed.
  all_merged[, pm25_conc_beacon_pats_threshold := dt_case_when(location_kitchen_threshold == 'Kitchen' ~ PATS_Kitchen,
                                                               location_kitchen_threshold != 'Kitchen' ~ PATS_LivingRoom,
                                                               !is.na(PATS_Ambient) ~ PATS_Ambient,
                                                               TRUE ~ mean(PATS_Ambient,na.rm = TRUE))]
  
  all_merged[, co_estimate_beacon_nearest_threshold := dt_case_when(location_kitchen_threshold == 'Kitchen' ~ CO_ppmKitchen,
                                                                    location_kitchen_threshold != 'Kitchen' ~ CO_ppmLivingRoom,
                                                                    !is.na(CO_ppmAmbient) ~ CO_ppmAmbient,
                                                                    TRUE ~ mean(CO_ppmAmbient,na.rm = TRUE))]
  
  all_merged_summary <- dplyr::group_by(all_merged,HHID) %>%
    dplyr::mutate(Date = as.Date(datetime[1], "%Y-%m-%d")) %>%
    dplyr::summarise_all(mean,na.rm = TRUE) %>%
    left_join(meta_emissions %>%
                mutate(HHID = HHID_full,
                       Date = as.Date(`Start time (Mobenzi Pre-placement)-- 1`)),
              by = c('HHID','Date'))
  
  return(list(all_merged,all_merged_summary))
  
}




