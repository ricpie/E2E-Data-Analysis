# fwrite(meta_data, file="r_scripts/tsi_dummy_meta_data.csv") 
dummy_meta_data <- fread("r_scripts/tsi_dummy_meta_data.csv")


tsi_ingest <- function(file, local_tz, output=c('raw_data', 'meta_data'),dummy='dummy_meta_data'){
  dummy_meta_data <- get(dummy, envir=.GlobalEnv)
  filename <- parse_filename_fun(file)
  badfileflag = 0
  #separate out raw, meta, and sensor data
  raw_data_temp <- fread(file, skip = 28,fill=TRUE)
  if (nrow(raw_data_temp)<10 && ncol(raw_data_temp)<6){badfileflag <- 1}else 
    if(ncol(raw_data_temp)==6){
      raw_data <- fread(file, skip = 29,fill=TRUE)
      raw_data<- raw_data[,1:6]
      setnames(raw_data, c("Date",	"Time",	"CO2_ppm",	"RH",	"Temp_C","CO_ppm"))
      raw_data<-raw_data[complete.cases(raw_data), ]}else{
        raw_data <- fread(file, skip = 29,fill=TRUE)
        raw_data<- raw_data[,1:7]
        setnames(raw_data, c("Date",	"Time",	"CO2_ppm",	"RH",	"Temp_C",	"Wetbulb_C","CO_ppm"))
        raw_data<-raw_data[complete.cases(raw_data), ]
        raw_data <- raw_data[,c(1:5,7)]}
  
  if(all(is.na(filename$sampleID)) || badfileflag == 1){
    #add "fake" metadata
    metadata <- dummy_meta_data
    metadata$file <- basename(file)
    metadata$qa_date = as.Date(Sys.Date())
    metadata$flags <- 'No usable data'
    metadata[, c('smry', 'analysis') := NULL]
    return(list(raw_data=NULL, meta_data=metadata))
  }else{
    
    
    raw_data$datetime <- paste(raw_data$Date,raw_data$Time)
    raw_data$datetime <- as.POSIXct(raw_data$datetime,tz=local_tz,tryFormats = c("%Y-%m-%d %H:%M:%OS","%m/%d/%Y %H:%M:%OS","%d/%m/%Y %H:%M:%OS"))
    #Adjust time by 12 hours if the time was set up incorrectly.
    if(chron(times=raw_data$Time[1])<chron(times='07:00:00')){
      raw_data$datetime <- raw_data$datetime + 60*60*12
    }else
      if(chron(times=raw_data$Time[1])>chron(times='20:00:00')){
        raw_data$datetime <- raw_data$datetime - 60*60*12
      }
    
    raw_data$CO2_ppm[raw_data$CO2_ppm %like% "Invalid"] = 5000
    raw_data$CO2_ppm <- as.numeric(raw_data$CO2_ppm)
    raw_data$CO_ppm[raw_data$CO_ppm %like% "Invalid"] = 500
    raw_data$CO_ppm <- as.numeric(raw_data$CO_ppm)
    
    #Sample rate. Time difference of samples, in minutes.
    sample_timediff = as.numeric(median(diff(raw_data$datetime,units='minutes')))
    
    #Sampling duration
    dur = difftime(max(raw_data$datetime),min(raw_data$datetime),units='hours')
    
    meta_data <- data.table(
      fullname=filename$fullname,
      basename=filename$basename,
      datetime_start = raw_data$datetime[1],
      qa_date = as.Date(Sys.Date()),
      sampleID=filename$sampleID ,
      filterID=filename$filterID,
      sampletype=filename$sampletype, 
      fieldworkerID=filename$fieldworkerID, 
      fieldworkernum=filename$fieldworkernum,
      HHID=filename$HHID,
      loggerID=filename$loggerID,
      samplerate_minutes = sample_timediff,
      sampling_duration = dur
    )
  }
  if(all(output=='meta_data')){return(meta_data)}else
    if(all(output=='raw_data')){return(raw_data)}else
      if(all(output == c('raw_data', 'meta_data'))){
        return(list(meta_data=meta_data, raw_data=raw_data))
      }
}


tsi_qa_fun <- function(file,meta_emissions,local_tz,output= 'meta_data'){
  ingest <- tsi_ingest(file, local_tz,output=c('raw_data', 'meta_data'))
  
  if(is.null(ingest)){return(NULL)}else{
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data)){
      return(meta_data)
    }else{
      raw_data <- ingest$raw_data
      #check the initial and final background concentrations
      first_minutes_bl_CO2 <- raw_data[, mean(head(CO2_ppm,5),na.rm = TRUE)]
      last_minutes_bl_CO2 <- raw_data[, mean(tail(CO2_ppm,5),na.rm = TRUE)]
      
      first_minutes_bl_CO <- raw_data[, mean(head(CO_ppm,5),na.rm = TRUE)]
      if(first_minutes_bl_CO>10){bg_start_flag=1}else{bg_start_flag=0}
      #check the last five minutes of the data set
      last_minutes_bl_CO <- raw_data[, mean(tail(CO_ppm,5),na.rm = TRUE)]
      if(last_minutes_bl_CO>10){bg_end_flag=1}else{bg_end_flag=0}
      
      #Calculate run average concentration flag
      Run_avg <- mean(raw_data$CO_ppm,na.rm = TRUE)
      Run_sd <- sd(raw_data$CO_ppm,na.rm = TRUE)
      
      #Raise flag if stdev of values is zero.
      nonresponsive_flag <- if(sd(raw_data$CO_ppm,na.rm = TRUE)==0 || sum(is.na(raw_data$CO_ppm)) > 0){1}else{0}
      
      #Raise flag if CO or CO2 max out
      CO_maxout_flag <- if(sum(raw_data$CO_ppm==500)>2){1}else{0}
      CO2_maxout_flag <- if(sum(raw_data$CO2_ppm==5000)>2){1}else{0}
      
      #Calculate AER from the CO2 data and background start and stop times
      meta_matched <- dplyr::left_join(meta_data,meta_emissions,by="HHID")
      
      raw_data_AER <- raw_data[raw_data$datetime<meta_matched$datetimedecayend & raw_data$datetime>meta_matched$datetimedecaystart,]
      meta_data <- AER_fun(file,meta_data,raw_data_AER,output = 'meta_data')
      raw_data$BGdecay <- 0
      raw_data$BGdecay[raw_data$datetime<meta_matched$datetimedecayend & raw_data$datetime>meta_matched$datetimedecaystart] = 1 #Points that come after the decay start
      
      
      meta_data = cbind(meta_data, first_minutes_bl_CO,first_minutes_bl_CO2,last_minutes_bl_CO,last_minutes_bl_CO2,Run_avg,Run_sd, bg_start_flag,bg_end_flag,nonresponsive_flag,CO2_maxout_flag,CO_maxout_flag)
      
      meta_data[, flag_total:=sum(bg_start_flag,bg_end_flag,nonresponsive_flag,CO2_maxout_flag,CO_maxout_flag), by=.SD]
      
      flags_str <- suppressWarnings(paste(melt(meta_data[,c(colnames(meta_data)[colnames(meta_data) %like% "flag"]), with=F])[value>0 & variable!="flag_total", gsub("_flag", "" , variable)] , collapse=", "))
      
      meta_data[, flags:=flags_str]
      
      as.data.frame(raw_data)
      
      
      #Plot the CO and CO2 data and save it
      tryCatch({ 
        #Prepare some text for looking at the ratios of high to low temps.
        plot_name = gsub(".xls",".png",basename(file))
        plot_name = paste0("QA Reports/Instrument Plots/TSI_",gsub(".XLS",".png",plot_name))
        tsiplot <- ggplot(raw_data,aes(y = CO_ppm, x = datetime)) +
          geom_line()+
          geom_line(data = raw_data, aes(y = CO2_ppm, x = datetime))+
          ggtitle(paste0('TSI_',basename(file))) + 
          labs(x="", y="ppm")+
          theme_minimal() 
        print(tsiplot)
        if(!file.exists(plot_name)){
          ggsave(filename=plot_name,plot=tsiplot,width = 8, height = 6)
        }
      }, error = function(error_condition) {
      }, finally={})
      
      return(meta_data)
    }
  }
}

tsi_metadata_fun <- function(file,output='raw_data',local_tz){
  ingest <- tsi_ingest(file,local_tz, output=c('raw_data', 'meta_data'))
  
  if(is.null(ingest)){return(NULL)}else{
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data)){
      return(meta_data)
    }else{
      raw_data <- ingest$raw_data
      
      #Add some metadata into the mix
      raw_data$sampleID <- meta_data$sampleID
      raw_data$loggerID <- meta_data$loggerID
      raw_data$HHID <- meta_data$HHID
      raw_data$sampletype <- meta_data$sampletype
      
      return(raw_data)
    }
  }
}



