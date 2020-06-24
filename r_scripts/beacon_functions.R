# fwrite(meta_data, file="r_scripts/beacon_dummy_meta_data.csv")
dummy_meta_data <- fread('r_scripts/beacon_dummy_meta_data.csv')

## Beacon import and analysis function
#One function to import data and do basic QA

#A separate function to build the deployment and assign presence
#Import, use Mobenzi to figure out which deployment it is from.  
#Correct time stamps if there are issues with them
#Save timeseries of locations with meta_data.
beacon_qa_fun = function(file, dummy='dummy_meta_data',mobeezi='mobenzi',timezone,preplacement){
   dummy_meta_data <- get(dummy, envir=.GlobalEnv)   
   base::message(file)
   if(nchar(basename(file)) < 30){message('File name not correct, check the file')
      return(NULL)}
   
   beacon_logger_data = fread(file,col.names = c('datetime',"MAC","RSSI"),skip = 1)
   
   #meta data
   filename <- parse_filename_fun(file)
   
   if(all(is.na(filename$sampleID)) | dim(beacon_logger_data)[1]<5){
      #add "fake" meta_data
      meta_data <- dummy_meta_data
      meta_data$fullname <- basename(file)
      meta_data$basename <- "Check filename"
      meta_data$qa_date = as.Date(Sys.Date())
      meta_data$datetime_start <- filename$datestart
      meta_data$qc <- 'bad'
      meta_data$flag_total <- NA
      return(meta_data=as.data.table(meta_data))
   }else{
      beacon_logger_data[,MAC := toupper(MAC)] 
      beacon_logger_data[,datetime := ymd_hms(datetime,tz=timezone)]
      keep <-names(tail(sort(table(beacon_logger_data[!is.na(RSSI)]$MAC)),3)) #Keep non-NA MAC addresses 
      beacon_logger_data_plot <- subset(beacon_logger_data, MAC %in% keep)
      keep = data.frame(BAID = keep)
      
      completeness_fl = 1;time_interval_fl = 1;beacon_presence_fl=0;beacon_time=1;startup_proc_fl=1;duration_fl=1
      #create a new data by each beacon emitter, specifying time interval of data logging, RSSI mean and logging time for each emitter
      beacon_logger_data_emitter = beacon_logger_data[,.(diff_time = as.numeric(names(sort(table(diff(datetime)),decreasing = T)[1])), sample_length = .N,
                                                         RSSI_mean = mean(RSSI))
                                                      , by = MAC]
      #is na exist for diff_time, change to 0 to flag but not clog the code
      beacon_logger_data_emitter[is.na(diff_time),diff_time := 0]
      beacon_logger_data_emitter[,sample_hour := (diff_time)/60/60*sample_length]
      beacon_logger_data_emitter[,sample_length:= NULL]
      beacon_logger_data_emitter <- beacon_logger_data_emitter[!(is.na(beacon_logger_data_emitter$RSSI_mean)),]
      beacon_logger_data_emitter[,time_rank := frank(-sample_hour)]
      beacon_logger_data_emitter = beacon_logger_data_emitter[time_rank %in% c(1:1)]
      
      #completeness flag: should have 6 hours of data logged for this logger
      if(sum(beacon_logger_data_emitter$sample_hour)<6){completeness_fl =1}else{completeness_fl = 0}
      
      #time_interval flag: data should be logged in every 15 - 45 second (some are 20 some are 40)
      if((sum(beacon_logger_data_emitter$diff_time<15) + sum(beacon_logger_data_emitter$diff_time>45))>0){time_interval_fl =1}else{time_interval_fl = 0}
      
      if(min(beacon_logger_data_emitter$sample_hour)<1){beacon_time = 1}else{beacon_time = 0}
      
      #startup_procedure flag: no beacon has RSSI greater than -75, loggers are placed to further away or obstruction
      if(max(beacon_logger_data_emitter$RSSI_mean) < -75){startup_proc_fl = 1}else{startup_proc_fl=0}
      #duration flag: check sample duration
      sample_duration = as.numeric(difftime( beacon_logger_data[nrow(beacon_logger_data),datetime],beacon_logger_data[1,datetime],units = 'days'))
      if(sample_duration<.8){duration_fl = 1}else{duration_fl=0}
      
      meta_data <- data.table(
         fullname=filename$fullname,basename=filename$basename,
         qa_date = as.Date(Sys.Date()),
         sampleID=filename$sampleID,
         datestart=filename$datestart,
         datetime_start = beacon_logger_data$datetime[1],
         sampletype=filename$sampletype, 
         fieldworkerID=filename$fieldworkerID, 
         fieldworkernum=filename$fieldworkernum, 
         HHID=filename$HHID,
         loggerID=filename$loggerID,
         qc = filename$flag,
         samplerate_minutes = beacon_logger_data_emitter$diff_time/60,
         sampling_duration = sample_duration
      )
      meta_data = cbind(meta_data,completeness_fl,time_interval_fl,beacon_presence_fl,beacon_time,startup_proc_fl,duration_fl)
      
      meta_data[, flag_total:=sum(completeness_fl,time_interval_fl,beacon_presence_fl,beacon_time,startup_proc_fl,duration_fl), by=.SD]
      
      # as.data.frame(beacon_logger_data)
      
      #Plot the Beacon data and save it
      tryCatch({ 
         #Prepare some text for looking at the ratios of high to low temps.
         equipment_IDs$MAC <- equipment_IDs$BAID
         beacon_logger_data_plot <- merge(beacon_logger_data_plot,equipment_IDs,by='MAC',all.x = TRUE)
         beacon_logger_data_plot$MACloggerid <- paste0(beacon_logger_data_plot$MAC,'; ',beacon_logger_data_plot$loggerID)
         plot_name = gsub(".txt",".png",basename(file))
         plot_name = paste0("QA Reports/Instrument Plots/BL_",gsub(".csv",".png",plot_name))
         
         ggg<- ggplot(beacon_logger_data_plot, aes(y = RSSI, x = datetime, colour=str_wrap(MACloggerid,18))) +
            geom_point(alpha = 0.15) +
            theme_minimal() +
            theme(legend.position = "right") +
            scale_x_datetime(date_labels = "%e-%b %H:%m") +
            theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10)) +
            ggtitle(plot_name) +
            theme(legend.title=element_blank(),axis.title.x = element_blank())
         # geom_smooth(method = 'loess',span = 60/(sample_duration*1440))
         
         ggsave(filename=plot_name, plot=ggg,width = 8, height = 5,dpi=200)
         
      }, error = function(error_condition) {
      }, finally={})
   }
   return(meta_data)    
}


beacon_import_fun <- function(file,timezone="UTC",preplacement=preplacement,beacon_time_corrections){
   base::message(file)
   beacon_logger_data = fread(file,col.names = c('datetime',"MAC","RSSI"),skip = 1)
   filename <- parse_filename_fun(file)
   
   if(all(is.na(filename$sampleID)) | dim(beacon_logger_data)[1]<5){return(NULL)}else{
      
      beacon_logger_data[,MAC := toupper(MAC)] 
      beacon_logger_data[,datetime := ymd_hms(datetime,tz=timezone)]
      
      #Fix time stamps if they are bad.
      if(beacon_logger_data$datetime[1]<as.POSIXct('2019-1-1')){
         newstartdatetime<-beacon_time_corrections$newstartdatetime[grepl(filename$basename,beacon_time_corrections$filename)]
         beacon_logger_data <- ShiftTimeStamp_unops(beacon_logger_data, newstartdatetime,timezone="UTC")
      }
      
      #Add some meta_data into the mix
      beacon_logger_data[,datetime := ymd_hms(datetime,tz=timezone)]
      beacon_logger_data[,datetime := floor_date(datetime, unit = "minutes")]
      beacon_logger_data[,sampleID := filename$sampleID]
      beacon_logger_data[,loggerID := filename$loggerID]
      beacon_logger_data[,HHID := filename$HHID]
      beacon_logger_data[,sampletype := filename$sampletype]
      beacon_logger_data[,qc := filename$flag]
      
      meta_data <- data.table(
         fullname=filename$fullname,basename=filename$basename,
         qa_date = as.Date(Sys.Date()),
         sampleID=filename$sampleID,
         datestart=filename$datestart,
         datetime_start = beacon_logger_data$datetime[1],
         sampletype=filename$sampletype, 
         fieldworkerID=filename$fieldworkerID, 
         fieldworkernum=filename$fieldworkernum, 
         HHID=filename$HHID,
         loggerID=filename$loggerID,
         qc = filename$flag
      )
      
      #Add some meta_data into the mix
      beacon_logger_data <- tag_timeseries_mobenzi(beacon_logger_data,preplacement,filename)
      beacon_logger_data <- tag_timeseries_emissions(beacon_logger_data,meta_emissions,meta_data,filename)
      beacon_logger_data<-beacon_logger_data[complete.cases(beacon_logger_data), ]
      attributes(beacon_logger_data$datetime)$tzone <- 'Africa/Nairobi'

      return(beacon_logger_data)
   }
}


##################################################
#Function to assign presence
#Don't care which beacon it is, as long as it is in the inventory
##################################################
#Algorithm: Using minute localization, but have two approaches to get there, either using the minute's strongest RSSI, or the strongest minute mean to assigne the room presence
#Also have a simplified approach that uses only the Kitchen's logger, and if the minute mean signal strength is greater than -60,, 70, or 80, then it assigned the kitchen, else the living room.
#And if there is no living room sample, or the participant is not home, then ambient is assigned.  In some cases where there was not ambient, the week's mean ambient value was assigned, but this is a large source of uncertainty.

beacon_deployment_fun = function(selected_preplacement,equipment_IDs,beacon_logger_raw,
                                 CO_calibrated_timeseries,pats_data_timeseries){
   # selected_preplacement<-mobenzilist[[5]]
   # selected_preplacement<-preplacement[preplacement$HHID == 'KE190-KE04',][1,]
   base::message(selected_preplacement$HHID)
   
   #Prep instrument IDs... take this part out to functionalize more generally later
   Beacons_mobenzi = data.frame(loggerID=c(id_slicer_add(selected_preplacement$BeaconID1,5), id_slicer_add(selected_preplacement$BeaconID2,5)))
   Beacons = merge(Beacons_mobenzi,equipment_IDs,by="loggerID")
   setnames(Beacons,"BAID","MAC",skip_absent=TRUE)
   Loggers <- data.frame(loggerID= c(selected_preplacement$BeaconLoggerIDKitchen,selected_preplacement$BeaconLoggerIDSecondary),
                         location = c('Kitchen',selected_preplacement$RoomTypeSecondary),
                         HHID = c(selected_preplacement$HHIDnumeric,selected_preplacement$HHIDnumeric))
   # Loggers = merge(Loggers,dplyr::filter(equipment_IDs,instrument %in% 'BeaconLogger'),by.x='loggerID',by.y='BAID')
   otherroom <- setdiff(Loggers$location,"Kitchen")
   
   #Get beacon data from loggers and beacons of interest
   beacon_data_temp <- beacon_logger_raw[grepl(paste(unique(Loggers$loggerID),collapse="|"),loggerID),]
   beacon_data_temp <- beacon_data_temp[grepl(paste(toupper(unique(Beacons$MAC)),collapse="|"),MAC),]  #To select only beacons in the selected_preplacement inventory
   # beacon_data_temp <- beacon_data_temp[grepl(paste(toupper(unique(equipment_IDs$BAID)),collapse="|"),MAC),]  #To select any beacons in the equipment inventory.  Use this one, as it allows for errors in naming.
   
   
   beacon_data_temp <- beacon_data_temp[datetime>selected_preplacement$WalkThrough1Start & datetime<selected_preplacement$start_datetime+3*86400,]
   # beacon_data_temp[,nearest_RSSI := round(max(RSSI)), by = c("loggerID","datetime")] 
   beacon_data_temp[,RSSI_minute_mean := round(mean(RSSI)), by = c("loggerID","datetime")] 
   beacon_data_temp <- unique(beacon_data_temp, by = c('datetime', 'loggerID'))
   beacon_data_temp <- merge(beacon_data_temp, Loggers,by = c('loggerID','HHID'))
   HHIDstr =  paste(strsplit(beacon_data_temp$sampleID[1],"-")[[1]][1:2],collapse = "-")
   beacon_data_temp <- dcast.data.table(beacon_data_temp,datetime + HHID + qc~ sampletype, value.var = c("location","loggerID","RSSI_minute_mean"))
   #if there is only one logger in the home,fill it in with repeat action, set all 
   if(dim(beacon_data_temp)[2]<7){
      beacon_data_temp = cbind(beacon_data_temp[,1:3],beacon_data_temp[,4],beacon_data_temp[,4],beacon_data_temp[,5],beacon_data_temp[,5],
                               beacon_data_temp[,6],beacon_data_temp[,6])
      beacon_data_temp[,4] = "Kitchen"
      beacon_data_temp[,5] = otherroom
      if('Kitchen' ==  names(sort(table(beacon_data_temp[,4]),decreasing=TRUE)[1])){
         beacon_data_temp[,9] = NA
         beacon_data_temp[,7] = ""
      } else {
         beacon_data_temp[,8] = NA
         beacon_data_temp[,6] = ""
         
      }
   }
   setnames(beacon_data_temp,c('datetime','HHID','qc','location_kitchen','location_livingroom','loggerID_Kitchen',
                               'loggerID_LivingRoom', 'RSSI_minute_mean_Kitchen', 'RSSI_minute_mean_LivingRoom'))
   
   
   #Fill in time series so there are no gaps.  This allows us to know the difference between missing data and when the people are away.
   rollup_pm = '1 min' #Time averaging 
   my_breaks = seq(floor_date(min(beacon_data_temp$datetime,na.rm=TRUE),'minutes'), 
                   floor_date(max(beacon_data_temp$datetime,na.rm=TRUE),'minutes'),
                   by = rollup_pm)
   
   beacon_data_temp = merge(data.table(datetime=my_breaks),beacon_data_temp, by='datetime',all.x = TRUE)
   beacon_data_temp$HHID <- na.locf(beacon_data_temp$HHID,na.rm = FALSE)
   beacon_data_temp$qc <- na.locf(beacon_data_temp$qc,na.rm = FALSE)
   beacon_data_temp$location_kitchen <- na.locf(beacon_data_temp$location_kitchen,na.rm = FALSE)
   beacon_data_temp$location_livingroom <- na.locf(beacon_data_temp$location_livingroom,na.rm = FALSE)
   beacon_data_temp$loggerID_Kitchen <- na.locf(beacon_data_temp$loggerID_Kitchen,na.rm = FALSE)
   beacon_data_temp$loggerID_LivingRoom <- na.locf(beacon_data_temp$loggerID_LivingRoom,na.rm = FALSE)
   
   beacon_data_temp <- dplyr::group_by(beacon_data_temp,datetime) %>%
      plyr::mutate(location_kitchen = as.character(location_kitchen)) %>% 
      plyr::mutate(location_livingroom = as.character(otherroom)) %>% 
      dplyr::mutate(location_nearest = case_when(is.na(RSSI_minute_mean_Kitchen) & is.na(RSSI_minute_mean_LivingRoom) ~ 'Ambient',
                                                 is.na(RSSI_minute_mean_Kitchen) & !is.na(RSSI_minute_mean_LivingRoom) ~ otherroom,
                                                 !is.na(RSSI_minute_mean_Kitchen) & is.na(RSSI_minute_mean_LivingRoom) ~ location_kitchen,
                                                 RSSI_minute_mean_Kitchen < RSSI_minute_mean_LivingRoom ~ otherroom,
                                                 RSSI_minute_mean_Kitchen > RSSI_minute_mean_LivingRoom ~ location_kitchen,
                                                 RSSI_minute_mean_Kitchen == RSSI_minute_mean_LivingRoom ~ 'Average',
                                                 TRUE ~ 'Ambient'),
                    location_kitchen_threshold = case_when(RSSI_minute_mean_Kitchen >= -70 ~ location_kitchen,
                                                           RSSI_minute_mean_Kitchen < -70 ~ otherroom,
                                                           is.na(RSSI_minute_mean_Kitchen)  ~ 'Ambient',
                                                           TRUE ~ 'Ambient'),
                    location_kitchen_threshold80 = case_when(RSSI_minute_mean_Kitchen >= -80 ~ location_kitchen,
                                                             RSSI_minute_mean_Kitchen < -80 ~ otherroom,
                                                             is.na(RSSI_minute_mean_Kitchen)  ~ 'Ambient',
                                                             TRUE ~ 'Ambient'),
                    HHIDstr = HHIDstr) %>%
      #If there is only one beacon logger, don't assign location_nearest.
      dplyr::ungroup() %>%
      dplyr::mutate(sumKitchen = sum(!is.na(RSSI_minute_mean_Kitchen))) %>%
      dplyr::mutate(sumLivingRoom = sum(!is.na(RSSI_minute_mean_LivingRoom))) %>%
      dplyr::mutate(location_nearest = case_when(sumKitchen>1 & sumLivingRoom > 1 ~ location_nearest)) %>% 
      dplyr::select(-sumKitchen,-sumLivingRoom) %>%
      as.data.table()
   
   
   return(beacon_data_temp)
}



beacon_walkthrough_function = function(beacon_logger_data,preplacement){
   
   #This should not be needed, as Noelle will provide a clean list of rapid survey SES values that correspond with Mobenzi preplacement survey, but for the time being...
   preplacement_nodupes <-preplacement[!duplicated(preplacement[5:30]),]
   
   beacon_walkthrough <- dplyr::left_join(beacon_logger_data %>%
                                             dplyr::mutate(HHIDnumeric = HHID), 
                                          preplacement_nodupes %>% 
                                             dplyr::mutate(loggerID_Kitchen = BeaconLoggerIDKitchen) %>%
                                             dplyr::select('HHID','HHIDnumeric','start_datetime','WalkThrough1Start','WalkThrough1End',
                                                           'WalkThrough2Start','WalkThrough2End','WalkThrough3Start','WalkThrough3End','loggerID_Kitchen'),
                                          by = c('HHIDnumeric','loggerID_Kitchen')) %>%
      dplyr::mutate(location_correct = case_when(
         # (datetime > WalkThrough1Start & datetime < WalkThrough1End) ~ 'Startup',
         (datetime > WalkThrough2Start & datetime < WalkThrough2End) ~ 'Kitchen',
         (datetime > WalkThrough3Start & datetime < WalkThrough3End) ~ 'Living Room'),
         # location_nearest = case_when(
         #    (RSSI_minute_mean_Kitchen > -70 & RSSI_minute_mean_LivingRoom > -70) ~ 'Startup',
         #    TRUE ~ location_nearest),
         # location_kitchen_threshold = case_when(
         #    (RSSI_minute_mean_Kitchen > -70 & RSSI_minute_mean_LivingRoom > -70) ~ 'Startup',
         #    TRUE ~ location_kitchen_threshold),
         # location_kitchen_threshold80 = case_when(
         #    (RSSI_minute_mean_Kitchen > -70 & RSSI_minute_mean_LivingRoom > -70) ~ 'Startup',
         #    TRUE ~ location_kitchen_threshold80),
         location_nearest = gsub('Secondary Kitchen','Living Room',location_nearest),
         location_kitchen_threshold = gsub('Secondary Kitchen','Living Room',location_kitchen_threshold),
         location_kitchen_threshold80 = gsub('Secondary Kitchen','Living Room',location_kitchen_threshold80)
      ) %>%
      dplyr::filter(!is.na(location_correct),
                    location_nearest != "NA",
                    location_kitchen_threshold  != "NA",
                    location_kitchen_threshold80 != "NA",
                    !is.na(loggerID_Kitchen),
                    !is.na(loggerID_LivingRoom)) %>%
      dplyr::distinct() %>%
      dplyr::select(-location_kitchen,-location_livingroom)
   
   contable_nearest <- prop.table(table(beacon_walkthrough$location_nearest,beacon_walkthrough$location_correct ), margin=2)*100
   contable_threshold <- prop.table(table(beacon_walkthrough$location_kitchen_threshold,beacon_walkthrough$location_correct ), margin=2)*100 
   contable_80 <- prop.table(table(beacon_walkthrough$location_kitchen_threshold80,beacon_walkthrough$location_correct ), margin=2)*100
   
   contable_nearest_byHH <- prop.table(table(beacon_walkthrough$location_nearest,beacon_walkthrough$location_correct,beacon_walkthrough$HHIDstr ), margin=2)*100
   # Switched: KE510-KE05 , ,  = KE231-KE005, ,    = KE190-KE004, ,  = KE186-KE004
   # 
   # Even: , ,  = KE507-KE005 , ,  = KE504-KE001, = KE229-KE011, ,, ,  = KE158-KE011, ,  = KE155-KE011, ,  = KE120-KE004, ,  = KE040-KE004, ,  = KE036-KE005, ,  = KE028-KE011, ,  = KE015-KE005
   # 
   # All kitchen: , ,  = KE502-KE004 , ,  = KE199-KE011
   # 
   # Bad/little data: , ,  = KE238-KE004
   
   
   
   uniqueHHIDs <- unique(beacon_walkthrough$HHIDstr)
   
   for(i in 1:length(uniqueHHIDs)){
      
      beacon_walkthrough_temp <- beacon_walkthrough[beacon_walkthrough$HHIDstr == uniqueHHIDs[i],]
      
      p1 <- pivot_longer(beacon_walkthrough_temp,
                         cols = starts_with("RSSI"),
                         names_to = "RSSI",
                         names_prefix = "RSSI",
                         values_to = "values",  
                         values_drop_na = TRUE) %>%
         mutate(RSSI = gsub('_',' ',RSSI)) %>%
         ggplot(aes(y = values, x = datetime)) +
         geom_point(aes(colour = RSSI,size=5), alpha=0.5) +
         theme_bw(20) +
         ggtitle(beacon_walkthrough_temp$HHID.y[1]) + 
         theme(legend.title=element_blank(),axis.title.x = element_blank()) +
         # theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))+
         ylab("RSSI")
      
      beacon_walkthrough_tile <- pivot_longer(beacon_walkthrough_temp,
                                              cols = starts_with("location"),
                                              names_to = "location",
                                              names_prefix = "location",
                                              values_to = "values",  
                                              values_drop_na = TRUE) %>%
         mutate(location = gsub('_',' ',location))
      levels(beacon_walkthrough_tile$location) = 1:length(unique(beacon_walkthrough_tile$location))
      
      p2 <- ggplot(aes(y = as.factor(location), x = datetime), data = beacon_walkthrough_tile) +
         geom_tile(aes(colour = values,fill=values), alpha=0.25) +
         theme_bw(20) +
         theme(legend.title=element_blank(),axis.title.x = element_blank()) +
         # scale_y_continuous(limits = c(20,100)) +
         # theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))+
         ylab("Predicted locations")
      
      plot_name = paste0("QA Reports/Instrument Plots/walkthrough_",beacon_walkthrough_temp$HHID.y[1],".png")
      png(plot_name,width = 1000, height = 800, units = "px")
      egg::ggarrange(p1,p2, heights = c(0.6,0.4))
      dev.off()
   }
}


