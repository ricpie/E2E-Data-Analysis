
## Beacon import and analysis function
#One function to import data and do basic QA

#A separate function to build the deployment and assign presence
#Use Mobenzi to figure out which deployment it is from.  
#Correct time stamps if there are issues with them
#Save timeseries of locations with meta_data.
beacon_deployment_fun = function(preplacement,equipment_IDs,tz,local_tz,beacon_data_timeseries,lascar_calibrated_timeseries,pats_data_timeseries){
  # preplacement<-mobenzilist[[11]]
  base::message(preplacement$HHID)
  #Prep instrument IDs... take this part out to functionalize more generally later
  Beacons_mobenzi = data.frame(loggerID=c(id_slicer_add(preplacement$BeaconID1,5), id_slicer_add(preplacement$BeaconID2,5)))
  Beacons = merge(Beacons_mobenzi,equipment_IDs,by="loggerID")
  setnames(Beacons,"BAID","MAC",skip_absent=TRUE)
  Loggers <- data.frame(loggerID= c(preplacement$BeaconLoggerIDKitchen,preplacement$BeaconLoggerIDSecondary),
                        location = c('Kitchen',preplacement$RoomTypeSecondary),
                        HHID = c(preplacement$HHIDnumeric,preplacement$HHIDnumeric))
  Loggers = merge(Loggers,equipment_IDs,by='loggerID')
  start_time <- as.POSIXct(strftime(paste(as.Date(preplacement$Start,format = "%d-%m-%Y"),preplacement$DevicesONTime),tz=local_tz),tz=local_tz)
  
  #Get beacon data from loggers and beacons of interest
  beacon_logger_data <- beacon_data_timeseries[grepl(paste(unique(Loggers$loggerID),collapse="|"),loggerID),]
  beacon_logger_data <- beacon_logger_data[grepl(paste(toupper(unique(Beacons$MAC)),collapse="|"),MAC),]
  beacon_logger_data <- beacon_logger_data[datetime>start_time & datetime<start_time+86400,]
  beacon_logger_data[,RSSI_minute_max := round(max(RSSI)), by = c("loggerID","datetime")] 
  beacon_logger_data <- unique(beacon_logger_data, by = c('datetime', 'loggerID'))
  beacon_logger_data <- as.data.table(dplyr::left_join(beacon_logger_data,Loggers,by=c("HHID","loggerID")) %>%
                                        dplyr::group_by(datetime) %>%
                                        dplyr::mutate(RSSI_localization = max(RSSI_minute_max)) %>%
                                        dplyr::mutate(n=n()) %>%
                                        dplyr::mutate(location = as.character(location))%>%
                                        dplyr::mutate(location = ifelse((RSSI_minute_max[1]==mean(RSSI_minute_max)) & (n == 2),"Average",location)) %>%
                                        dplyr::mutate(location = ifelse(is.na(RSSI_minute_max),"Ambient",location)) %>%
                                        dplyr::mutate(sampletype = ifelse((RSSI_minute_max[1]==mean(RSSI_minute_max)) & (n == 2),"M",sampletype)) %>%
                                        dplyr::mutate(sampletype = ifelse(is.na(RSSI_minute_max),"A",sampletype)) %>%
                                        dplyr::arrange(datetime) %>%
                                        dplyr::select(-instrument,-RSSI) %>%
                                        dplyr::filter((RSSI_minute_max==max(RSSI_minute_max) | is.na(RSSI_minute_max))) %>%
                                        dplyr::distinct(datetime,.keep_all=TRUE))
  
  
  if(nrow(beacon_logger_data)==0){
    base::message('No Beacon data found, various possible issue!  May check the Mobenzi start date/time or Beacon file timestamps or IDs.  Also possible that no Beacon emitter found, please check Beacon inventory and Beacon logger file, Check ID_missing_beacons for more information')
    base::message(paste('Beacon IDs found: ', paste(Beacons_mobenzi$loggerID,collapse = ' ')))
    base::message(paste('Mobenzi Start Date: ', preplacement$Start))
    
  }else{
    
    #merge with realtime data for indirect measures (there will be some duplicate values)
    # Merge beacon data with Lascar, by HHID and location, but also save both environments' measurements in the time series.
    # beacon_logger_data = merge(beacon_logger_data, pats_data_timeseries[,c("datetime","deviceID","filter_id","ECM_location","ECM_PM")], by.x = c("datetime3","monitor_env"),by.y = c("datetime2","ECM_location"), all.x = T, all.y = F)
    
    lascar_calibrated_subset <- lascar_calibrated_timeseries[datetime>start_time & datetime<start_time+86400,]
    
    beacon_logger_COmerged = merge(beacon_logger_data, lascar_calibrated_subset, by.x = c("datetime","HHID","sampletype"),by.y = c("datetime","HHID","sampletype"), all.x = T, all.y = F)
    beacon_logger_COmerged[,sampletype := "C"] 
    beacon_logger_COmerged = merge(beacon_logger_COmerged, lascar_calibrated_subset, by.x = c("datetime","HHID","sampletype"),by.y = c("datetime","HHID","sampletype"), all.x = T, all.y = F)
    setnames(beacon_logger_COmerged, old=c("CO_ppm.x","CO_ppm.y"), new=c("CO_ppm_indirect", "CO_ppm_direct"))
    
    
  }
  
  #Plot the Beacon data and save it
  tryCatch({ 
    #Plot the position.
    
    # beacon_plot <- ggplot(beacon_logger_COmerged, aes(x = as.POSIXct(datetime), y = RSSI_localization, color = location))+
    #    geom_point()+
    #    labs(x = "Datetime",y = "RSSI")+
    #    theme_bw()
    samplename <- paste0("Loc_",preplacement$HHID,"_",as.character(preplacement$UNOPS_Date))
    beacon_logger_COmerged_melted = melt(beacon_logger_COmerged,measure.vars = c("CO_ppm_indirect", "CO_ppm_direct"))
    p1 <- qplot(CO_ppm_indirect,CO_ppm_direct, data = beacon_logger_COmerged) + ggtitle(samplename)
    p2 <- qplot(datetime,value, data = beacon_logger_COmerged_melted,geom = c("point", "smooth"),color=variable) + theme(legend.position="bottom")+
      scale_color_viridis_d()
    p3 <- qplot(datetime, RSSI_localization, data = beacon_logger_COmerged,color = location) + theme(legend.position="bottom")
    beacon_plot <- arrangeGrob(p1, p2, p3, nrow=3) #generates g
    
    plot_name = paste0("QA Reports/Instrument Plots/Loc_",preplacement$HHID,"_",as.character(preplacement$UNOPS_Date),'.png')
    if(!file.exists(plot_name)){
      ggsave(filename=plot_name,plot=beacon_plot,width = 8, height = 6)
    }
  }, error = function(e) {
  }, finally={})
  
  return(beacon_logger_data)    
}
