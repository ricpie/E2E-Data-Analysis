

##################################################
#Merge the time series data sets and calculate exposure estimates
##################################################

all_merge_fun = function(preplacement,beacon_logger_data,
                         CO_calibrated_timeseries,tsi_timeseries,pats_data_timeseries,ecm_dot_data){
  
  #####ECM data prep
  # ecm_dot_data needs to go wide on dupes and sums
  ecm_dot_data[,(c('other_people_use_n','pm_accel','file','mission_name',
                   'indoors','shared_cooking_area','pm_rh','pm_temp','dot_temperature','qc','samplestart','sampleend')) := NULL]  
  
  #This chunk aggregates data from multiple stoves of the same type into a single stove (e.g. two lpg stoves becomes one, TRUE cooking state is kept if either one is true)
  #Uses the max compliance (will be of the cook), available.
  ecm_dot_data <-  dplyr::group_by(ecm_dot_data,HHID,stove_type,datetime,sampletype) %>%
    dplyr::arrange(desc(cooking)) %>%
    dplyr::filter(row_number()==1) %>%
    dplyr::arrange(datetime) %>%
    dplyr::group_by(HHID,datetime) %>%
    dplyr::mutate(pm_compliant = max(pm_compliant)) %>%
    dplyr::ungroup() 
  
  #Take the ecm data wide so we can get a single time series, onto which we can merge other data streams.
  wide_ecm_dot_data <- pivot_wider(ecm_dot_data,
                                   names_from = c(sampletype),
                                   values_from = c(pm25_conc),
                                   names_prefix = 'PM25') %>%
    pivot_wider(names_from = c(stove_type),
                values_from = c(cooking),
                names_prefix = 'sums') %>% 
    dplyr::select(-sumsNA) %>% 
    as.data.table()
  
  
  #####Lascar data prep
  # lascar data needs to go wide on location# lascar data needs to go wide on location (sampletype)
  # remove a few cols
  CO_calibrated_timeseries <- readRDS("Processed Data/CO_calibrated_timeseries.rds")[qc == 'good']
  
  CO_calibrated_timeseries[,(c('qc','ecm_tags','fullname','basename','datetime_start','qa_date','sampleID','loggerID',
                               'filterID','fieldworkerID','fieldworkernum','samplerate_minutes','sampling_duration_hrs','file','flags','emission_startstop')) := NULL]
  CO_calibrated_timeseries[, sampletype := dt_case_when(sampletype == 'Cook Dup' ~ 'Cook',
                                                        sampletype =='1m Dup' ~ '1m',
                                                        sampletype == '2m Dup' ~ '2m',
                                                        sampletype == 'Living Room Dup' ~ 'LivingRoom',
                                                        sampletype == 'Living Room' ~ 'LivingRoom',
                                                        sampletype == 'Ambient Dup' ~ 'Ambient',
                                                        sampletype == 'Kitchen Dup' ~ 'Kitchen',           
                                                        TRUE ~ sampletype)]
  CO_calibrated_timeseries[, sampletype := paste0("CO_ppm", sampletype)]
  
  #Use averages if there are duplicates
  CO_calibrated_timeseries <-  dplyr::group_by(CO_calibrated_timeseries,HHID,sampletype,datetime) %>%
    dplyr::mutate(CO_ppm = mean(CO_ppm)) %>%
    dplyr::filter(row_number()==1) %>%
    dplyr::ungroup() %>% as.data.table() 
  
  CO_calibrated_timeseries_hap <- CO_calibrated_timeseries[HHID!='777' & HHID!='888']
  CO_calibrated_timeseries_ambient <- CO_calibrated_timeseries[HHID=='777'
                                                               ][,CO_ppmAmbient:=CO_ppm
                                                                 ][,c('HHID','CO_ppm','sampletype','emission_tags','HHID_full'):=NULL]
  
  wide_co_data <- dcast.data.table(CO_calibrated_timeseries_hap,datetime + HHID + HHID_full~ sampletype, value.var = c("CO_ppm"))
  
  #Merge house/personal time serires with ambient data (based on date time only, not HHID)
  wide_co_data = merge(wide_co_data,CO_calibrated_timeseries_ambient, by.x = c("datetime"),
                       by.y = c("datetime"), all.x = T, all.y = F,)
  rm(CO_calibrated_timeseries,CO_calibrated_timeseries_ambient,CO_calibrated_timeseries_hap)
  
  
  
  #####PATS data prep
  
  pats_data_timeseries <- readRDS("Processed Data/pats_data_timeseries.rds")[qc == 'good']
  
  pats_data_timeseries[,c('V_power', 'degC_air','%RH_air','CO_PPM','status','ref_sigDel','low20avg','loggerID',
                          'high320avg','motion','emission_tags','sampleID','qc','emission_startstop','OG PM','UPAS ON') := NULL]
  
  pats_data_timeseries[,measure := "pm25_conc"]
  pats_data_timeseries[, sampletype := paste0("PATS_", sampletype)]
  
  #Group data from duplicates into a mean value.
  pats_data_timeseries <-  dplyr::group_by(pats_data_timeseries,HHID_full,sampletype,datetime) %>%
    dplyr::mutate(pm25_conc = mean(pm25_conc)) %>%
    dplyr::filter(row_number()==1) %>%
    dplyr::ungroup()  %>% as.data.table() 
  
  pats_data_timeseries_hap <- pats_data_timeseries[HHID!='777' & HHID!='888']
  pats_data_timeseries_ambient <- pats_data_timeseries[HHID=='777'
                                                       ][,pm25_concAmbient:=pm25_conc
                                                         ][,c('measure','HHID','pm25_conc','sampletype','pm25_conc_unadjusted','HHID_full'):=NULL]
  rm(pats_data_timeseries)
  
  wide_pats_data <- dcast.data.table(pats_data_timeseries_hap,datetime + HHID + HHID_full ~ sampletype, value.var = c("pm25_conc"))
  
  #Merge house/personal time serires with ambient data (based on date time only, not HHID)
  wide_pats_data = merge(wide_pats_data,pats_data_timeseries_ambient, by.x = c("datetime"),
                         by.y = c("datetime"), all.x = T, all.y = F,)
  setnames(wide_pats_data, "pm25_concAmbient", "PATS_Ambient")
  rm(pats_data_timeseries_ambient,pats_data_timeseries_hap)
  
  
  
  #####Prep beacon data (only exists for cook)
  
  beacon_logger_data<- readRDS("Processed Data/beacon_logger_data.rds")[qc == 'good']
  # beacon_logger_data[,measure := "beacon"]
  beacon_logger_data[,c('location_kitchen','location_livingroom','loggerID_Kitchen','qc','loggerID_LivingRoom','HHIDstr','measure') := NULL]
  beacon_logger_data <- beacon_logger_data[!duplicated(beacon_logger_data),]
  
  
  #####Prep emissions, (tsi) 
  
  tsi_timeseries <- as.data.table(readRDS("Processed Data/tsi_timeseries.rds"))
  # tsi_timeseries[,measure := "emissions"]
  tsi_timeseries[,(c('sampleID','sampletype','RH','Date')) := NULL]
  
  
  #####Merge dot, ecm, pats, lascar, tsi data.  Preplacement survey not merged here.
  
  all_merged = merge(wide_pats_data,wide_co_data, by.x = c("datetime","HHID_full"),
                     by.y = c("datetime","HHID_full"), all.x = T, all.y = F)
  all_merged = merge(all_merged,beacon_logger_data, by.x = c("datetime","HHID.x"),
                     by.y = c("datetime","HHID"), all.x = T, all.y = F)
  all_merged = merge(all_merged,tsi_timeseries, by.x = c("datetime","HHID.x"),
                     by.y = c("datetime","HHID"), all.x = T, all.y = F)
  all_merged <- merge(all_merged,wide_ecm_dot_data, by.x = c("datetime","HHID_full"),
                      by.y = c("datetime","HHID"), all.x = T, all.y = F)
  
  #If there is no ECM kitchen data, use the PATS kitchen data.  PATS_kitchen data has been adjusted.
  all_merged <- dplyr::mutate(as.data.table(all_merged),ECM_kitchen = PM25Kitchen,
                              PM25Kitchen = case_when(is.na(ECM_kitchen) ~ PATS_Kitchen,
                                                      TRUE ~ ECM_kitchen)) %>%
    dplyr::rename(HHIDnumeric = HHID.x,
                  HHID = HHID_full) %>%
    dplyr::rowwise() %>% mutate(PM25_kitch_lr_mean = mean(c(PM25Kitchen,PATS_LivingRoom),na.rm=T),
                                CO_kitch_lr_mean =  mean(c(CO_ppmKitchen,CO_ppmLivingRoom),na.rm=T)) %>%
    dplyr::mutate(HHID = case_when(HHIDnumeric == 152 ~ "KE152-KE04",
                                   HHIDnumeric == 157 ~ "KE157-KE06",
                                   HHIDnumeric == 85 ~ "KE085-KE03",
                                   TRUE ~ HHID)) %>%
    dplyr::select(-HHID.y) %>%
    as.data.table()
  
  
  
  # tester <-all_merged %>% group_by(HHID) %>% summarise_each(funs(mean(., na.rm = TRUE))) %>% print()
  #### Assign exposure based on nearest beacon (use both methods still)
  #Pref. use the ECM if available.  If the kitchen ECM data is NAN, use the kitchen PATS
  #If the livingroom is PATS is NAN, use the ambient data
  meanPATSAmbient = mean(all_merged$PATS_Ambient,na.rm = TRUE)
  meanCOAmbient = mean(all_merged$CO_ppmAmbient,na.rm = TRUE)
  #Indirect using ECM data in kitchen (nearest beacon algo).  NA locations get NA PM values
  all_merged[, pm25_conc_beacon_nearest_ecm := dt_case_when(location_nearest == 'Kitchen' ~ PM25Kitchen, #PM25 kitchen uses mostly ECM, but some corrected PATS data.
                                                            location_nearest == 'Ambient' & !is.na(PATS_Ambient) ~ PATS_Ambient, 
                                                            location_nearest == 'Ambient' & is.na(PATS_Ambient) ~ meanPATSAmbient,  
                                                            location_nearest == 'Average'  ~ PM25_kitch_lr_mean,  
                                                            location_nearest != 'Kitchen' & !is.na(PATS_LivingRoom) ~ PATS_LivingRoom,
                                                            location_nearest != 'Kitchen' & is.na(PATS_LivingRoom) & !is.na(PATS_Ambient) ~ PATS_Ambient,
                                                            location_nearest != 'Kitchen' & is.na(PATS_LivingRoom) & is.na(PATS_Ambient) ~ meanPATSAmbient)]
  # table(all_merged$location_nearest,is.na(all_merged$co_estimate_beacon_nearest),useNA = "ifany")  
  
  #Estimate uses kitchen and living room co, (nearest beacon algo)
  all_merged[, co_estimate_beacon_nearest := dt_case_when(location_nearest == 'Kitchen' ~ CO_ppmKitchen,
                                                          location_nearest == 'Ambient' & !is.na(CO_ppmAmbient) ~ CO_ppmAmbient, 
                                                          location_nearest == 'Ambient' & is.na(CO_ppmAmbient) ~ meanCOAmbient,  
                                                          location_nearest == 'Average'  ~  CO_kitch_lr_mean,  
                                                          location_nearest != 'Kitchen' & !is.na(CO_ppmLivingRoom) ~ CO_ppmLivingRoom,
                                                          location_nearest != 'Kitchen' & is.na(CO_ppmLivingRoom) & !is.na(CO_ppmAmbient) ~ CO_ppmAmbient,  #Use ambient if livingroom is NAN.
                                                          location_nearest != 'Kitchen' & is.na(CO_ppmLivingRoom) & is.na(CO_ppmAmbient) ~ meanCOAmbient)]  #Use ambient if livingroom is NAN.
  
  ####This batch uses kitchen if we have signal greater than x RSSI in the kitchen, otherwise, living room, otherwise ambient
  #Estimate uses kitchen ecm data and living room pats data, and ambient pats if needed.
  all_merged[, pm25_conc_beacon_nearestthreshold_ecm := dt_case_when(location_kitchen_threshold == 'Kitchen' & !is.na(PM25Kitchen) ~ PM25Kitchen,
                                                                     location_kitchen_threshold == 'Ambient' & !is.na(PATS_Ambient) ~ PATS_Ambient, 
                                                                     location_kitchen_threshold == 'Ambient' & is.na(PATS_Ambient) ~ meanPATSAmbient,  
                                                                     location_kitchen_threshold == 'Average'  ~ PM25_kitch_lr_mean,  
                                                                     location_kitchen_threshold != 'Kitchen' & !is.na(PATS_LivingRoom) ~ PATS_LivingRoom,
                                                                     location_kitchen_threshold != 'Kitchen' & is.na(PATS_LivingRoom) & !is.na(PATS_Ambient) ~ PATS_Ambient,
                                                                     location_kitchen_threshold != 'Kitchen' & is.na(PATS_LivingRoom) & is.na(PATS_Ambient) ~ meanPATSAmbient)]
  
  
  all_merged[, pm25_conc_beacon_nearestthreshold_ecm80 := dt_case_when(location_kitchen_threshold80 == 'Kitchen' & !is.na(PM25Kitchen) ~ PM25Kitchen,
                                                                       location_kitchen_threshold80 == 'Ambient' & !is.na(PATS_Ambient) ~ PATS_Ambient, 
                                                                       location_kitchen_threshold80 == 'Ambient' & is.na(PATS_Ambient) ~ meanPATSAmbient,  
                                                                       location_kitchen_threshold80 == 'Average'  ~ PM25_kitch_lr_mean,  
                                                                       location_kitchen_threshold80 != 'Kitchen' & !is.na(PATS_LivingRoom) ~ PATS_LivingRoom,
                                                                       location_kitchen_threshold80 != 'Kitchen' & is.na(PATS_LivingRoom) & !is.na(PATS_Ambient) ~ PATS_Ambient,
                                                                       location_kitchen_threshold80 != 'Kitchen' & is.na(PATS_LivingRoom) & is.na(PATS_Ambient) ~ meanPATSAmbient)]  #Use ambient if livingroom is NAN.
  
  
  
  all_merged[, co_estimate_beacon_nearest_threshold := dt_case_when(location_kitchen_threshold == 'Kitchen' ~ CO_ppmKitchen,
                                                                    location_kitchen_threshold == 'Ambient' & !is.na(CO_ppmAmbient) ~ CO_ppmAmbient, 
                                                                    location_kitchen_threshold == 'Ambient' & is.na(CO_ppmAmbient) ~ meanCOAmbient,  
                                                                    location_kitchen_threshold == 'Average'  ~ CO_kitch_lr_mean,  
                                                                    location_kitchen_threshold != 'Kitchen' & !is.na(CO_ppmLivingRoom) ~ CO_ppmLivingRoom,
                                                                    location_kitchen_threshold != 'Kitchen' & is.na(CO_ppmLivingRoom) & !is.na(CO_ppmAmbient) ~ CO_ppmAmbient,  #Use ambient if livingroom is NAN.
                                                                    location_kitchen_threshold != 'Kitchen' & is.na(CO_ppmLivingRoom) & is.na(CO_ppmAmbient) ~ meanCOAmbient)]  
  
  all_merged <- all_merged[!duplicated(all_merged[,c(35:44,1:12)]),]
  all_merged[,CO_kitch_lr_mean := NULL]
  all_merged[,PM25_kitch_lr_mean := NULL]
  
  all_merged_intensive <- all_merged[!is.na(all_merged$PATS_Kitchen),] # keep the larger dataset for intensive data removing PATS kitchen data that is NA, joining with Dot data
  all_merged_intensive[,sumstraditional_non_manufactured:=NULL]
  all_merged_intensive[,sumslpg:=NULL]
  all_merged_intensive[,sumstraditional_manufactured:=NULL]
  all_merged_intensive[,`sumscharcoal jiko`:=NULL]
  
  # Organize dot data to merge it with intensive data set.
  wide_dot_data = readRDS("../Data/analysis-20200421/wide_dot_data.RDS")  
  
  all_merged_intensive <- merge(all_merged_intensive,wide_dot_data, by.x = c("datetime","HHID"),
                                by.y = c("datetime","HHID"), all.x = T, all.y = F)
  
  
  all_merged = all_merged[!is.na(all_merged$PM25Cook),]
  all_merged = all_merged[!is.nan(all_merged$PM25Cook),]  
  
  all_merged_summary <- dplyr::group_by(all_merged,HHID,Date) %>%
    dplyr::summarise(Start_datetime = min(datetime,na.rm = TRUE),
                     End_datetime = max(datetime,na.rm = TRUE),
                     meanPM25Cook = mean(PM25Cook,na.rm = TRUE),
                     tsiCO2ppm = mean(CO2_ppm,na.rm = TRUE),
                     mean_compliance = mean(pm_compliant,na.rm = TRUE),
                     meanPM25Kitchen = mean(PM25Kitchen,na.rm = TRUE),
                     meanPM25LivingRoom = mean(PATS_LivingRoom,na.rm = TRUE),
                     meanPM25Ambient =  mean(PATS_Ambient,na.rm = TRUE),
                     meanCO_ppmCook = mean(CO_ppmCook,na.rm = TRUE),
                     meanCO_ppmKitchen = mean(CO_ppmKitchen,na.rm = TRUE),
                     meanCO_ppmLivingRoom = mean(CO_ppmLivingRoom,na.rm = TRUE),
                     meanCO_ppmAmbient = mean(CO_ppmAmbient,na.rm = TRUE),
                     meanCO_ppmEmissions = mean(CO_ppm,na.rm = TRUE),
                     meanCO2_ppmEmissions = mean(CO2_ppm,na.rm = TRUE),
                     sum_TraditionalManufactured_minutes = sum(sumstraditional_manufactured,na.rm = TRUE),
                     sum_charcoal_jiko_minutes = sum(`sumscharcoal jiko`,na.rm = TRUE),
                     sum_traditional_non_manufactured_minutes = sum(sumstraditional_non_manufactured,na.rm = TRUE),
                     sum_lpg_minutes = sum(sumslpg,na.rm = TRUE),
                     meanpm25_indirect_nearest = mean(pm25_conc_beacon_nearest_ecm,na.rm = TRUE),
                     meanpm25_indirect_nearest_threshold = mean(pm25_conc_beacon_nearestthreshold_ecm,na.rm = TRUE),
                     meanpm25_indirect_nearest_threshold80 = mean(pm25_conc_beacon_nearestthreshold_ecm80,na.rm = TRUE),
                     meanCO_indirect_nearest = mean(co_estimate_beacon_nearest,na.rm = TRUE),
                     meanCO_indirect_nearest_threshold = mean(co_estimate_beacon_nearest_threshold,na.rm = TRUE),
                     na_TraditionalManufactured_minutes = sum(is.na(sumstraditional_manufactured)),
                     na_charcoal_jiko_minutes = sum(is.na(`sumscharcoal jiko`)),
                     na_traditional_non_manufactured = sum(is.na(sumstraditional_non_manufactured)),
                     na_lpg = sum(is.na(sumslpg),na.rm = TRUE),
                     countPM25Kitchen = sum(!is.na(PM25Kitchen)),
                     countPM25Cook = sum(!is.na(PM25Cook)),
                     countPATS_LivingRoom = sum(!is.na(PATS_LivingRoom)),
                     countPM25Cook = sum(!is.na(PM25Cook)),
                     countlocation_nearest = sum(!is.na(location_nearest)),
                     countlocation_nearestthreshold = sum(!is.na(location_kitchen_threshold))) %>%
    dplyr::mutate(sum_TraditionalManufactured_minutes = case_when(na_TraditionalManufactured_minutes<0.8*1440 ~ sum_TraditionalManufactured_minutes,
                                                                  T ~ as.integer(NA)),
                  sum_charcoal_jiko_minutes = case_when(na_charcoal_jiko_minutes<0.8*1440 ~ sum_charcoal_jiko_minutes,
                                                        T ~ as.integer(NA)),
                  sum_traditional_non_manufactured_minutes = case_when(na_traditional_non_manufactured<0.8*1440 ~ sum_traditional_non_manufactured_minutes,
                                                                       T ~ as.integer(NA)),
                  sum_lpg_minutes = case_when(na_lpg<0.8*1440 ~ sum_lpg_minutes,
                                              T ~ as.integer(NA)))     %>%
    dplyr::left_join( all_merged %>%
                        dplyr::group_by(HHID,Date,emission_startstop) %>%
                        dplyr::summarise(CookingmeanPM25Cook = mean(PM25Cook,na.rm = TRUE),
                                         CookingmeanPM25Kitchen = mean(PM25Kitchen,na.rm = TRUE),
                                         CookingmeanPM25Kitchen1m = mean(PATS_1m,na.rm = TRUE),
                                         CookingmeanPM25Kitchen2m = mean(PATS_2m,na.rm = TRUE),
                                         CookingmeanPM25LivingRoom = mean(PATS_LivingRoom,na.rm = TRUE),
                                         CookingmeanPM25Ambient =  mean(PATS_Ambient,na.rm = TRUE),
                                         CookingmeanCO_ppmCook = mean(CO_ppmCook,na.rm = TRUE),
                                         CookingmeanCO_ppmKitchen = mean(CO_ppmKitchen,na.rm = TRUE),
                                         CookingmeanCO_ppmKitchen1m = mean(CO_ppm1m,na.rm = TRUE),
                                         CookingmeanCO_ppmKitchen2m = mean(CO_ppm2m,na.rm = TRUE),
                                         CookingmeanCO_ppmLivingRoom = mean(CO_ppmLivingRoom,na.rm = TRUE),
                                         CookingmeanCO_ppmAmbient = mean(CO_ppmAmbient,na.rm = TRUE),
                                         Cookingsum_TraditionalManufactured_minutes = sum(sumstraditional_manufactured,na.rm = TRUE),
                                         Cookingsum_charcoal_jiko_minutes = sum(`sumscharcoal jiko`,na.rm = TRUE),
                                         Cookingsum_traditional_non_manufactured = sum(sumstraditional_non_manufactured,na.rm = TRUE),
                                         Cookingsum_lpg = sum(sumslpg,na.rm = TRUE),
                                         Cookingmeanpm25_indirect_nearest = mean(pm25_conc_beacon_nearest_ecm,na.rm = TRUE),
                                         Cookingmeanpm25_indirect_nearest_threshold = mean(pm25_conc_beacon_nearestthreshold_ecm,na.rm = TRUE),
                                         CookingmeanCO_indirect_nearest = mean(co_estimate_beacon_nearest,na.rm = TRUE),
                                         CookingmeanCO_indirect_nearest_threshold = mean(co_estimate_beacon_nearest_threshold,na.rm = TRUE),
                                         Cookingna_TraditionalManufactured_minutes = sum(is.na(sumstraditional_manufactured)),
                                         Cookingna_charcoal_jiko_minutes = sum(is.na(`sumscharcoal jiko`)),
                                         Cookingna_traditional_non_manufactured = sum(is.na(sumstraditional_non_manufactured)),
                                         Cookingna_lpg = sum(is.na(sumslpg))) %>%
                        dplyr::mutate(Cookingsum_TraditionalManufactured_minutes = case_when(Cookingna_TraditionalManufactured_minutes<1 ~ Cookingsum_TraditionalManufactured_minutes,
                                                                                      T ~ as.integer(NA)),
                                      Cookingsum_charcoal_jiko_minutes = case_when(Cookingna_charcoal_jiko_minutes<1 ~ Cookingsum_charcoal_jiko_minutes,
                                                                            T ~ as.integer(NA)),
                                      Cookingsum_traditional_non_manufactured = case_when(Cookingna_traditional_non_manufactured<1 ~ Cookingsum_traditional_non_manufactured,
                                                                                           T ~ as.integer(NA)),
                                      Cookingsum_lpg = case_when(Cookingna_lpg<1 ~ Cookingsum_lpg,
                                                                  T ~ as.integer(NA))) %>%
                        dplyr::filter(emission_startstop == 'cooking') %>%
                        dplyr::select(-Cookingna_TraditionalManufactured_minutes,-Cookingna_charcoal_jiko_minutes,-Cookingna_traditional_non_manufactured,Cookingna_lpg)
                        ,
                      by = c('HHID','Date'))   %>%
    dplyr::left_join(all_merged_intensive %>%
                       dplyr::group_by(HHID) %>%
                       dplyr::summarise(Intensive_meanPM25Kitchen = mean(PATS_Kitchen,na.rm = TRUE),
                                        Intensive_N_PM25Kitchen = sum(!is.na(PATS_Kitchen)),
                                        Intensive_meanPM25LivingRoom = mean(PATS_LivingRoom,na.rm = TRUE),
                                        Intensive_N_PM25LivingRoom = sum(!is.na(PATS_LivingRoom)),
                                        Intensive_meanCO_ppmKitchen = mean(CO_ppmKitchen,na.rm = TRUE),
                                        Intensive_N_CO_ppmKitchen = sum(!is.na(CO_ppmKitchen)),
                                        Intensive_meanCO_ppmLivingRoom = mean(CO_ppmLivingRoom,na.rm = TRUE),
                                        Intensive_N_CO_ppmLivingRoom = sum(!is.na(CO_ppmLivingRoom)),
                                        Intensive_sum_TraditionalManufactured_minutes = sum(sumstraditional_manufactured,na.rm = TRUE),
                                        Intensive_N_sumstraditional_manufactured = sum(!is.na(sumstraditional_manufactured)),
                                        Intensive_sum_charcoal_jiko_minutes = sum(`sumscharcoal jiko`,na.rm = TRUE),
                                        Intensive_N_sum_charcoal_jiko = sum(!is.na(`sumscharcoal jiko`)),
                                        Intensive_sum_traditional_non_manufactured = sum(sumstraditional_non_manufactured,na.rm = TRUE),
                                        Intensive_N_sum_traditional_non_manufactured = sum(!is.na(sumstraditional_non_manufactured)),
                                        Intensive_sum_lpg = sum(sumslpg,na.rm = TRUE),
                                        Intensive_N_sum_lpg = sum(!is.na(sumslpg)),
                                        Intensivena_TraditionalManufactured_minutes = sum(is.na(sumstraditional_manufactured)),
                                        Intensivena_charcoal_jiko_minutes = sum(is.na(`sumscharcoal jiko`)),
                                        Intensivena_traditional_non_manufactured = sum(is.na(sumstraditional_non_manufactured)),
                                        Intensivena_lpg = sum(is.na(sumslpg))) %>%
                       dplyr::mutate(Intensive_sum_TraditionalManufactured_minutes = case_when(Intensivena_TraditionalManufactured_minutes<0.8*1440 ~ Intensive_sum_TraditionalManufactured_minutes,
                                                                                               T ~ as.integer(NA)),
                                     Intensive_sum_charcoal_jiko_minutes = case_when(Intensivena_charcoal_jiko_minutes<0.8*1440 ~ Intensive_sum_charcoal_jiko_minutes,
                                                                                     T ~ as.integer(NA)),
                                     Intensive_sum_traditional_non_manufactured = case_when(Intensivena_traditional_non_manufactured<0.8*1440 ~ Intensive_sum_traditional_non_manufactured,
                                                                                            T ~ as.integer(NA)),
                                     Intensive_sum_lpg = case_when(Intensivena_lpg<0.8*1440 ~ Intensive_sum_lpg,
                                                                   T ~ as.integer(NA))) %>%
                       dplyr::select(-Intensivena_TraditionalManufactured_minutes,-Intensivena_charcoal_jiko_minutes,-Intensivena_traditional_non_manufactured,Intensivena_lpg),
                     # dplyr::filter(!is.na(Date)),
                     by = c('HHID')) %>%
    dplyr::left_join(meta_emissions %>%
                       dplyr::select(stovetype,HHID_full,`Start time (Mobenzi Pre-placement)-- 1`) %>%
                       dplyr::mutate(HHID = HHID_full,
                                     Date = as.Date(`Start time (Mobenzi Pre-placement)-- 1`)),
                     by = c('HHID','Date'))
  
  write.xlsx(all_merged_summary,'Processed Data/all_merged_summary_mj.xlsx')
  
  return(list(all_merged,all_merged_summary))
}




