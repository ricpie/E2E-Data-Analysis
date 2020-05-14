

##################################################
#Function to assign presence
#Don't care which beacon it is, as long as it is in the inventory
##################################################

beacon_deployment_fun = function(selected_preplacement,equipment_IDs,beacon_logger_raw,
                                 CO_calibrated_timeseries,pats_data_timeseries){
  # selected_preplacement<-mobenzilist[[12]]
  base::message(selected_preplacement$HHID)
  
  #Prep instrument IDs... take this part out to functionalize more generally later
  Beacons_mobenzi = data.frame(loggerID=c(id_slicer_add(selected_preplacement$BeaconID1,5), id_slicer_add(selected_preplacement$BeaconID2,5)))
  Beacons = merge(Beacons_mobenzi,equipment_IDs,by="loggerID")
  setnames(Beacons,"BAID","MAC",skip_absent=TRUE)
  Loggers <- data.frame(loggerID= c(selected_preplacement$BeaconLoggerIDKitchen,selected_preplacement$BeaconLoggerIDSecondary),
                        location = c('Kitchen',selected_preplacement$RoomTypeSecondary),
                        HHID = c(selected_preplacement$HHIDnumeric,selected_preplacement$HHIDnumeric))
  Loggers = merge(Loggers,dplyr::filter(equipment_IDs,instrument %in% 'BeaconLogger'),by.x='loggerID',by.y='BAID')
  otherroom <- setdiff(Loggers$location,"Kitchen")
  
  #Get beacon data from loggers and beacons of interest
  beacon_data_temp <- beacon_logger_raw[grepl(paste(unique(Loggers$loggerID),collapse="|"),loggerID),]
  beacon_data_temp <- beacon_data_temp[grepl(paste(toupper(unique(Beacons$MAC)),collapse="|"),MAC),]  #To select only beacons in the selected_preplacement inventory
  # beacon_data_temp <- beacon_data_temp[grepl(paste(toupper(unique(equipment_IDs$BAID)),collapse="|"),MAC),]  #To select any beacons in the equipment inventory.  Use this one, as it allows for errors in naming.
  
  beacon_data_temp <- beacon_data_temp[datetime>selected_preplacement$start_datetime & datetime<selected_preplacement$start_datetime+86400,]
  beacon_data_temp[,nearest_RSSI := round(max(RSSI)), by = c("loggerID","datetime")] 
  beacon_data_temp[,RSSI_minute_min := round(min(RSSI)), by = c("loggerID","datetime")] 
  beacon_data_temp <- unique(beacon_data_temp, by = c('datetime', 'loggerID'))
  beacon_data_temp <- dplyr::left_join(beacon_data_temp,Loggers,by=c("HHID","loggerID")) %>%
    dplyr::distinct(datetime,.keep_all=TRUE) %>%
    dplyr::mutate(location = as.character(location)) %>%
    dplyr::group_by(datetime,loggerID) %>%
    dplyr::mutate(RSSI_max = max(nearest_RSSI)) %>%
    dplyr::mutate(RSSI_min = min(RSSI_minute_min)) %>%
    dplyr::mutate(n=n()) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(location_nearest = ifelse((nearest_RSSI[1]==mean(nearest_RSSI)) & (n == 2),"Average",location)) %>%
    # dplyr::mutate(location_min = ifelse((RSSI_minute_min[1]==mean(RSSI_minute_min)) & (n == 2),"Average",location)) %>%
    dplyr::mutate(location = ifelse(is.na(nearest_RSSI),"Ambient",location)) %>%
    dplyr::mutate(sampletype = ifelse((nearest_RSSI[1]==mean(nearest_RSSI)) & (n == 2),"M",sampletype)) %>%
    dplyr::mutate(sampletype = ifelse(is.na(nearest_RSSI),"A",sampletype)) %>%
    dplyr::arrange(datetime) %>%
    dplyr::group_by(datetime) %>%
    dplyr::mutate(Kitchen_RSSI = ifelse(location == "Kitchen",RSSI_max,NA)) %>%
    tidyr::fill(Kitchen_RSSI, .direction = "down") %>%
    dplyr::mutate(location_kitchen_threshold = ifelse(Kitchen_RSSI>-70,"Kitchen",otherroom)) %>%
    dplyr::filter(nearest_RSSI==max(nearest_RSSI) | is.na(nearest_RSSI)) %>%
    dplyr::select(-instrument,-RSSI_max,-RSSI_min,-RSSI,-n) %>%
    dplyr::distinct(datetime,.keep_all=TRUE)
  
  beacon_data_temp <- as.data.table(beacon_data_temp)
  
  return(beacon_data_temp)
}


##################################################
#Merge the time series data sets and calculate exposure estimates
##################################################

all_merge_fun = function(preplacement,beacon_logger_data,
                         CO_calibrated_timeseries,tsi_timeseries,pats_data_timeseries,ecm_dot_data){
  
  #Merge ecm with everything, sequentially
  
  # ecm_dot_data needs to go wide on dupes and sums
  ecm_dot_data[,(c('other_people_use_n','pm_accel','file','mission_name',
                   'indoors','shared_cooking_area','pm_rh','pm_temp','pm_compliant','dot_temperature')) := NULL]  
  
    #This chunk aggregates data from multiple stoves of the same type into a single stove (e.g. two lpg stoves becomes one, TRUE cooking state is kept if either one is true)
  ecm_dot_data <-  dplyr::group_by(ecm_dot_data,HHID,stove_type,datetime,pm_location) %>%
    dplyr::arrange(desc(cooking)) %>%
    dplyr::filter(row_number()==1) %>%
    dplyr::arrange(datetime) %>%
    dplyr::ungroup() %>% as.data.table() 
  
  #Take the ecm data wide so we can get a single time series, onto which we can merge other data streams.
  wide_ecm_dot_data <- pivot_wider(ecm_dot_data,
                                   names_from = c(pm_location),
                                   values_from = c(3),
                                   names_prefix = 'PM25') %>%
    pivot_wider(names_from = c(stove_type),
                values_from = c(3)) %>% as.data.table()

  
  # lascar data needs to go wide on location# lascar data needs to go wide on location (sampletype)
  # remove a few cols
  CO_calibrated_timeseries <- readRDS("Processed Data/CO_calibrated_timeseries.rds")
  
  CO_calibrated_timeseries[qc == 'good',][,(c('ecm_tags','emission_tags','fullname','basename','datetime_start','qa_date','sampleID',#'loggerID',
                               'filterID','fieldworkerID','fieldworkernum','samplerate_minutes','sampling_duration_hrs','file','flags')) := NULL]
  CO_calibrated_timeseries <- CO_calibrated_timeseries[qc == 'good']
  CO_calibrated_timeseries[, sampletype := dt_case_when(sampletype == 'Cook Dup' ~ 'Cook',
                                                        sampletype =='1m Dup' ~ '1m',
                                                        sampletype == '2m Dup' ~ '2m',
                                                        sampletype == 'Living Room Dup' ~ 'Living room',
                                                        sampletype == 'Kitchen Dup' ~ 'Kitchen',           
                                                        TRUE ~ sampletype)]
  
  #Use averages if there are duplicates
  CO_calibrated_timeseries <-  dplyr::group_by(CO_calibrated_timeseries,HHID,sampletype,datetime) %>%
    dplyr::mutate(CO_ppm = mean(CO_ppm)) %>%
    dplyr::filter(row_number()==1) %>%
    dplyr::ungroup() %>%
    dplyr::select(-loggerID) %>% as.data.table() 
  
  CO_calibrated_timeseries[, sampletype := paste0("CO_", sampletype)]
  
  wide_co_data <- dcast.data.table(CO_calibrated_timeseries,datetime + HHID ~ sampletype, value.var = c("CO_ppm"))
  
  # Merge with pats widened data
  pats_data_timeseries <- readRDS("Processed Data/pats_data_timeseries.rds")
  
  pats_data_timeseries[qc == 'good',][,(c('V_power', 'degC_air','%RH_air','CO_PPM','status','ref_sigDel','low20avg',
                           'high320avg','motion','ecm_tags','emission_tags','sampleID',
                           'qc')) := NULL]
  
  #Group data from duplicates into a mean value?  Would need to change to be the same sampletype, then group by hhid, datetime, sampletype and calculate the mean, and keep only one of the rows
  pats_data_timeseries <-  dplyr::group_by(pats_data_timeseries,HHID,sampletype,datetime) %>%
    dplyr::mutate(PM_Estimate = mean(PM_Estimate)) %>%
    dplyr::filter(row_number()==1) %>%
    dplyr::ungroup() %>%
    dplyr::select(-loggerID) %>% as.data.table() 
  
  pats_data_timeseries[, sampletype := paste0("PATS_", sampletype)]
  
  wide_pats_data <- dcast.data.table(pats_data_timeseries,datetime + HHID ~ sampletype, value.var = c("PM_Estimate"))
  
  
  
  # Merge with beacon data (doesn't need to be wide, only exists for cook)
  beacon_logger_data <- beacon_logger_data[qc == 'good',][,c('loggerID.y','MAC','RSSI_minute_min','sampleID',
                                                             'sampletype','loggerID','qc','ecm_tags','emission_tags','Kitchen_RSSI','location') := NULL]
  
  # merge with emissions, (tsi) 
  tsi_timeseries <- tsi_timeseries[qc == 'good',][,(c('Date','Time','sampleID','qc','sampletype','RH','Temp_C')) := NULL]
  
  
  # merge with preplacement data subset or emissions subset?
  pre <- preplacement[,c('...')]
  
  
  all_merged = merge(wide_ecm_dot_data,wide_co_data, by.x = c("datetime","pm_hhid_numeric"),
                     by.y = c("datetime","HHID"), all.x = T, all.y = F)
  all_merged = merge(all_merged,wide_pats_data, by.x = c("datetime","pm_hhid_numeric"),
                     by.y = c("datetime","HHID"), all.x = T, all.y = F)
  all_merged <- merge(all_merged,beacon_logger_data, by.x = c("datetime","pm_hhid_numeric"),
                      by.y = c("datetime","HHID"), all.x = T, all.y = F)
  all_merged = merge(all_merged,tsi_timeseries, by.x = c("datetime","pm_hhid_numeric"),
                     by.y = c("datetime","HHID"), all.x = T, all.y = F)
  
  
  all_merged<-all_merged[order(rank(pm_hhid_numeric)),]
  
  # use boolean logic to assign exposure based on nearest beacon (use both methods still)
  
  all_merged[, pm_estimate_beacon_nearest_ecm := dt_case_when(location_nearest == 'Kitchen' ~ PM25Kitchen,
                                                  location_nearest != 'Kitchen' ~ PATS_L,
                                                        TRUE ~ PATS_A)]
  all_merged[, pm_estimate_beacon_pats := dt_case_when(location_nearest == 'Kitchen' ~ PATS_K2,
                                                      location_nearest != 'Kitchen' ~ PATS_L,
                                                      TRUE ~ PATS_A)]
  all_merged[, co_estimate_beacon_nearest := dt_case_when(location_nearest == 'Kitchen' ~ CO_Kitchen,
                                                          location_nearest != 'Kitchen' ~ `CO_Living Room`,
                                                          TRUE ~ CO_Ambient)]
  
  all_merged[, pm_estimate_beacon_nearestthreshold_ecm := dt_case_when(location_kitchen_threshold == 'Kitchen' ~ PM25Kitchen,
                                                                       location_kitchen_threshold != 'Kitchen' ~ PATS_L,
                                                              TRUE ~ PATS_A)]
  all_merged[, pm_estimate_beacon_pats := dt_case_when(location_kitchen_threshold == 'Kitchen' ~ PATS_K2,
                                                       location_kitchen_threshold != 'Kitchen' ~ PATS_L,
                                                       TRUE ~ PATS_A)]
  all_merged[, co_estimate_beacon_nearest := dt_case_when(location_kitchen_threshold == 'Kitchen' ~ CO_Kitchen,
                                                          location_kitchen_threshold != 'Kitchen' ~ `CO_Living Room`,
                                                          TRUE ~ CO_Ambient)]
  
  # merge with emissions, (tsi) preplacement
  
  
  
  
  
  



  

  
  
  
  
  
  
  
  
  
  if(nrow(beacon_data_temp)==0){
    base::message('No Beacon data found, various possible issue!  May check the Mobenzi start date/time or Beacon file timestamps or IDs.  
                  Also possible that no Beacon emitter found, please check Beacon inventory and Beacon logger file, Check ID_missing_beacons for more information')
    base::message(paste('Beacon IDs found: ', paste(Beacons_mobenzi$loggerID,collapse = ' ')))
    base::message(paste('Mobenzi Start Date: ', selected_preplacement$Start))
    return(list(beacon_data_temp=NULL, beacon_logger_COmerged=NULL))
    
  }else { #Calculate exposure estimates
    lascar_calibrated_subset <- CO_calibrated_timeseries[datetime>selected_preplacement$start_datetime & datetime<selected_preplacement$start_datetime+86400,]
    beacon_logger_COmerged = merge(beacon_data_temp,lascar_calibrated_subset, by.x = c("datetime","HHID","sampletype"),
                                   by.y = c("datetime","HHID","sampletype"), all.x = T, all.y = F)
    beacon_logger_COmerged[,sampletype := "C"] 
    beacon_logger_COmerged = merge(beacon_logger_COmerged, lascar_calibrated_subset, by.x = c("datetime","HHID","sampletype"),
                                   by.y = c("datetime","HHID","sampletype"), all.x = T, all.y = F)
    setnames(beacon_logger_COmerged, old=c("CO_ppm.x","CO_ppm.y"), new=c("CO_ppm_indirect", "CO_ppm_direct"))
    
    lascar_calibrated_subset <- CO_calibrated_timeseries[datetime>selected_preplacement$start_datetime & datetime<selected_preplacement$start_datetime+86400,]
    beacon_logger_COmerged = merge(beacon_data_temp,lascar_calibrated_subset, by.x = c("datetime","HHID","sampletype"),
                                   by.y = c("datetime","HHID","sampletype"), all.x = T, all.y = F)
    beacon_logger_COmerged[,sampletype := "C"] 
    beacon_logger_COmerged = merge(beacon_logger_COmerged, lascar_calibrated_subset, by.x = c("datetime","HHID","sampletype"),
                                   by.y = c("datetime","HHID","sampletype"), all.x = T, all.y = F)
    setnames(beacon_logger_COmerged, old=c("CO_ppm.x","CO_ppm.y"), new=c("CO_ppm_indirect", "CO_ppm_direct"))
  }
  
  tryCatch({ 
    #Plot the position.
    samplename <- paste0("Loc_",selected_preplacement$HHID,"_",as.character(selected_preplacement$UNOPS_Date))
    beacon_logger_COmerged_melted = melt(beacon_logger_COmerged,measure.vars = c("CO_ppm_indirect", "CO_ppm_direct"))
    p1 <- qplot(CO_ppm_indirect_nearest,CO_ppm_direct, data = beacon_logger_COmerged) + ggtitle(samplename)
    p2 <- qplot(datetime,CO_ppm_indirect_kitchen_threshold, data = beacon_logger_COmerged_melted,geom = c("point", "smooth"),color=variable) + theme(legend.position="bottom")+
      scale_color_viridis_d()
    p3 <- qplot(datetime, Kitchen_RSSI, data = beacon_logger_COmerged,color = location_kitchen_threshold) + theme(legend.position="bottom")
    p4 <- qplot(datetime, nearest_RSSI, data = beacon_logger_COmerged,color = location_nearest) + theme(legend.position="bottom")
    beacon_plot <- arrangeGrob(p1, p2, p3,p4, nrow=2,ncol=2) #generates g
    
    plot_name = paste0("QA Reports/Instrument Plots/Loc_",selected_preplacement$HHID,"_",as.character(selected_preplacement$UNOPS_Date),'.png')
    if(!file.exists(plot_name)){
      ggsave(filename=plot_name,plot=beacon_plot,width = 8, height = 6)
    }
  }, error = function(e) {
  }, finally={})
  
  if(all(output=='beacon_logger_COmerged')){return(beacon_logger_COmerged)}else
    if(all(output=='beacon_data_temp')){return(beacon_data_temp)}else
      if(all(output==c('beacon_data_temp', 'beacon_logger_COmerged'))){
        return(list( beacon_data_temp=beacon_data_temp,beacon_logger_COmerged=beacon_logger_COmerged))
      }
}




