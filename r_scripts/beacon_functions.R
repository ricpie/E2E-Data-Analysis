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
   
   equipment_IDs <- readRDS("Processed Data/equipment_IDs.rds")
   
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
      keep <-names(tail(sort(table(beacon_logger_data$MAC)),2)) 
      beacon_logger_data_plot <- subset(beacon_logger_data, MAC %in% keep)
      
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
      beacon_logger_data[,datetime := round_date(datetime, unit = "minutes")]
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
      
      # return(as.data.table(beacon_logger_data))
   }
}



