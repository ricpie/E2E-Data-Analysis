# fwrite(meta_data, file="r_scripts/beacon_dummy_meta_data.csv")
dummy_meta_data <- fread('r_scripts/beacon_dummy_meta_data.csv')

## Beacon import and analysis function
#One function to import data and do basic QA

#A separate function to build the deployment and assign presence
#Import, use Mobenzi to figure out which deployment it is from.  
#Correct time stamps if there are issues with them
#Save timeseries of locations with meta_data.
beacon_qa_fun = function(file, dummy='dummy_meta_data',mobeezi='mobenzi',tz){
   dummy_meta_data <- get(dummy, envir=.GlobalEnv)   
   base::message(file)
   if(nchar(basename(file)) < 30){message('File name not correct, check the file')
      return(NULL)}
   
   equipment_IDs <- readRDS("Processed Data/equipment_IDs.R")
   
   beacon_logger_data = fread(file,col.names = c('datetime',"MAC","RSSI"),skip = 1)
   beacon_logger_data[,MAC := toupper(MAC)] 
   beacon_logger_data[,datetime := ymd_hms(datetime,tz=tz)]
   
   #meta data
   filename <- parse_filename_fun(file)
   
   if(all(is.na(filename$sampleID))){
      #add "fake" meta_data
      meta_data <- dummy_meta_data
      meta_data$fullname <- basename(file)
      meta_data$basename <- "Check filename"
      meta_data$qa_date = as.Date(Sys.Date())
      meta_data$datetime_start <- beacon_logger_data$datetime[1]
      meta_data$flag_total <- NA
      return(meta_data=as.data.frame(meta_data))
   }else{
      
      keep <-names(tail(sort(table(beacon_logger_data$MAC)),2)) 
      beacon_logger_data_plot <- subset(beacon_logger_data, MAC %in% keep)
      
      completeness_fl = 1;time_interval_fl = 1;beacon_presence_fl=1;beacon_time=1;startup_proc_fl=1;duration_fl=1
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
         samplerate_minutes = beacon_logger_data_emitter$diff_time/60,
         sampling_duration = sample_duration
      )
      meta_data = cbind(meta_data,completeness_fl,time_interval_fl,beacon_presence_fl,beacon_time,startup_proc_fl,duration_fl)
      
      meta_data[, flag_total:=sum(completeness_fl,time_interval_fl,beacon_presence_fl,beacon_time,startup_proc_fl,duration_fl), by=.SD]
      
      # as.data.frame(beacon_logger_data)
      
      #Plot the Beacon data and save it
      tryCatch({ 
         #Prepare some text for looking at the ratios of high to low temps.
         plot_name = gsub(".txt",".png",basename(file))
         plot_name = paste0("QA Reports/Instrument Plots/BL_",gsub(".csv",".png",plot_name))
         png(filename=plot_name,width = 550, height = 480, res = 100)
         plot(beacon_logger_data_plot$datetime, beacon_logger_data_plot$RSSI, main=plot_name,
              type = "p", ylab="RSSI",prob=TRUE,cex.main = .6,cex = .5)
         grid(nx = 5, ny = 10, col = "lightgray", lty = "dotted",
              lwd = par("lwd"), equilogs = TRUE)
         dev.off()
      }, error = function(error_condition) {
      }, finally={})
   }
   return(meta_data)    
}


beacon_import_fun <- function(file,tz,preplacement,beacon_time_corrections){
   
   beacon_logger_data = fread(file,col.names = c('datetime',"MAC","RSSI"),skip = 1)
   beacon_logger_data[,MAC := toupper(MAC)] 
   beacon_logger_data[,datetime := ymd_hms(datetime,tz=tz)]
   beacon_logger_data[,datetime := round_date(datetime, unit = "minutes")]
   
   filename <- parse_filename_fun(file)
   
   #Fix time stamps if they are bad.
   if(beacon_logger_data$datetime[1]<as.POSIXct('2019-1-1')){
      newstartdatetime<-beacon_time_corrections$newstartdatetime[grepl(filename$basename,beacon_time_corrections$filename)]
      beacon_logger_data <- ShiftTimeStamp(beacon_logger_data, newstartdatetime,tz)
   }
   
   
   if(is.null(beacon_logger_data)){return(NULL)}else{
      
      #Add some meta_data into the mix
      beacon_logger_data$sampleID <- filename$sampleID
      beacon_logger_data$loggerID <- filename$loggerID
      beacon_logger_data$HHID <- filename$HHID
      beacon_logger_data$sampletype <- filename$sampletype
      beacon_logger_data<-beacon_logger_data[complete.cases(beacon_logger_data), ]
      
      return(as.data.frame(beacon_logger_data))
   }
}


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
   # sampletypes = c("1","2","K","C","L","A")
   
   #Get beacon data from loggers and beacons of interest
   beacon_logger_data <- beacon_data_timeseries[grepl(paste(unique(Loggers$loggerID),collapse="|"),beacon_data_timeseries$loggerID),]
   beacon_logger_data <- beacon_logger_data[grepl(paste(toupper(unique(Beacons$MAC)),collapse="|"),beacon_data_timeseries$MAC),]
   beacon_logger_data <- beacon_logger_data[beacon_data_timeseries$datetime>start_time & beacon_data_timeseries$datetime<start_time+86400,]
   beacon_logger_data[,RSSI_minute_max := round(max(beacon_data_timeseries$RSSI)), by = c("loggerID","datetime")] 
   beacon_logger_data <- unique(beacon_logger_data, by = c('datetime', 'loggerID'))
   beacon_logger_data <- dplyr::left_join(beacon_logger_data,Loggers,by=c("HHID","loggerID")) %>%
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
      dplyr::distinct(datetime,.keep_all=TRUE)
   
   
   if(nrow(beacon_logger_data)==0){
      base::message('No Beacon data found, various possible issue!  May check the Mobenzi start date/time or Beacon file timestamps or IDs.  Also possible that no Beacon emitter found, please check Beacon inventory and Beacon logger file, Check ID_missing_beacons for more information')
      base::message(paste('Beacon IDs found: ', paste(Beacons_mobenzi$loggerID,collapse = ' ')))
      base::message(paste('Mobenzi Start Date: ', preplacement$Start))
      
   }else{
      
      #merge with realtime data for indirect measures (there will be some duplicate values)
      # Merge beacon data with Lascar, by HHID and location, but also save both environments' measurements in the time series.
      # beacon_logger_data = merge(beacon_logger_data, pats_data_timeseries[,c("datetime","deviceID","filter_id","ECM_location","ECM_PM")], by.x = c("datetime3","monitor_env"),by.y = c("datetime2","ECM_location"), all.x = T, all.y = F)

      
      # lascar_calibrated_subset <- lascar_calibrated_timeseries[lascar_calibrated_timeseries$HHID==preplacement$HHIDnumeric[1] |
                                                                  # lascar_calibrated_timeseries$HHID==777,]
      lascar_calibrated_subset <- lascar_calibrated_timeseries[lascar_calibrated_timeseries$datetime>start_time & lascar_calibrated_timeseries$datetime<start_time+86400,]
      
      beacon_
      logger_data = merge(beacon_logger_data, lascar_calibrated_subset, by.x = c("datetime","HHID","sampletype"),by.y = c("datetime","HHID","sampletype"), all.x = T, all.y = F)
      
   }
   
   #Plot the Beacon data and save it
   tryCatch({ 
      #Plot the position.
      
      beacon_plot <- ggplot(beacon_logger_data, aes(x = as.POSIXct(datetime), y = RSSI_localization, color = location))+
         geom_point()+
         labs(x = "Datetime",y = "RSSI")+
         theme_bw()
      print(beacon_plot)
      # p1 <- qplot(mpg, wt, data = mtcars, colour = cyl)
      # p2 <- qplot(mpg, data = mtcars) + ggtitle("title")
      # grid.arrange(p1, p2, nrow = 1)
      
      plot_name = paste0("QA Reports/Instrument Plots/Loc_",preplacement$HHID,"_",as.character(preplacement$UNOPS_Date),'.png')
      if(!file.exists(plot_name)){
         ggsave(filename=plot_name,plot=beacon_plot,width = 8, height = 6)
      }
   }, error = function(e) {
   }, finally={})
   
   return(beacon_logger_data)    
}


