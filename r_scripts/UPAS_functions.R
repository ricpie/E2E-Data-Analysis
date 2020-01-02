# fwrite(meta_data, file="r_scripts/upas_dummy_meta_data.csv") 
# file = file_list[1]

dummy_meta_data <- fread('r_scripts/upas_dummy_meta_data.csv')

UPAS_ingest_fun <- function(file, output=c('raw_data', 'meta_data'),tz, dummy='dummy_meta_data'){
  
  dummy_meta_data <- get(dummy, envir=.GlobalEnv)
  
  #separate out raw, meta, and sensor data
  raw_data=read.table(file, header=T,quote = "",sep=",",skip=59,na.string=c("","null","NaN"))[,c(1:26)] %>%
    dplyr::mutate(SampleTime=as.POSIXct(SampleTime,format='%H:%M:%S',tz=tz)) %>%
    dplyr::mutate(DateTimeLocal=as.POSIXct(DateTimeLocal,format='%Y-%m-%dT%H:%M:%S',tz=tz)) %>%
    dplyr::mutate(DateTimeUTC=as.POSIXct(DateTimeUTC,format='%Y-%m-%dT%H:%M:%S'))
  raw_data = as.data.table(raw_data)
  
  setnames(raw_data, c("SampleTime","UnixTime","DateTimeUTC","datetime","VolumetricFlowRate","SampledVolume","PumpT","PCBT","FdpT","PumpP","PCBP","FdPdP","PumpRH",  "AtmoRho", "PumpPow1","PumpPow2","PumpV",   "MassFlow","BFGvolt","BFGenergy","GPSlat","GPSlon","GPSalt","GPSsat","GPSspeed","GPShdop" ))
  
  meta_data_in <- as.data.table(read.csv(file, nrow=50, header=T, stringsAsFactor=F))
  filename <- parse_filename_fun(file)
  
  if(all(is.na(filename$sampleID))){
    #add "fake" meta_data
    meta_data <- dummy_meta_data
    meta_data$file <- basename(file)
    meta_data$qa_date <- as.Date(Sys.Date())
    meta_data$flags <- 'No usable data'
    meta_data$datestart <- filename$datestart
    meta_data$datetime_start <- NA
    meta_data[, c('smry', 'analysis') := NULL]
    return(list(raw_data=NULL, meta_data=meta_data))
  }else{
    meta_data <- data.table(
      qa_date = as.Date(Sys.Date()),
      fullname=filename$fullname,
      basename=filename$basename,
      sampleID=filename$sampleID ,
      filterID=filename$filterID,
      sampletype=filename$sampletype, 
      fieldworkerID=filename$fieldworkerID, 
      fieldworkernum=filename$fieldworkernum, 
      HHID=filename$HHID,
      datestart = filename$datestart,
      datetime_start = raw_data$datetime[1],
      loggerID=filename$loggerID,
      shutdown_reason=as.numeric(meta_data_in$VALUE[32]),
      dutycycle=meta_data_in[PARAMETER=='DutyCycle',as.numeric(VALUE)],
      AverageVolumetricFlowRate = meta_data_in[PARAMETER=='AverageVolumetricFlowRate',as.numeric(VALUE)],
      RunTimeHrs=as.numeric(meta_data_in$VALUE[35]),
      software_vers = meta_data_in$VALUE[2],
      filterID_upas = meta_data_in[PARAMETER=='SampleName',VALUE],
      app_version = meta_data_in[PARAMETER=='AppVersion', VALUE]
    )
  }
  
  if(all(output=='meta_data')){return(meta_data)}else
    if(all(output=='raw_data')){return(raw_data)}else
      if(all(output == c('raw_data', 'meta_data'))){
        return(list(meta_data=meta_data, raw_data=raw_data))}
}


UPAS_qa_fun <- function(file, tz){
  ingest <- UPAS_ingest_fun(file, output=c('raw_data', 'meta_data'),tz)
  filename <- parse_filename_fun(file)
  
  if(is.null(ingest)){return(NULL)}else{
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data)){
      return(meta_data)
    }else{
      raw_data <- ingest$raw_data
      raw_data_long <- melt(raw_data[, c(1,2,3,4, 5, 8, 10, 12, 13, 14, 19:25)], id.var='datetime')
      #create a rounded dt variable
      raw_data[, round_time:=round_date(datetime, 'hour')]
      
      #inlet pressure flag
      inletp_flag <- if(raw_data_long[variable=='PumpP' & !is.na(value), any(value>1200)]){1}else{0}
      #temperature range flag
      temp_flag <- if(raw_data_long[variable=='PCBT' & !is.na(value), any(!(value %between% temp_thresholds))]){1}else{0}
      rh_flag <- if(!is.na(raw_data_long[variable=='PumpRH' & !is.na(value), (mean(value)>rh_threshold)])&raw_data_long[variable=='PumpRH' & !is.na(value), (mean(value)>rh_threshold)]){1}else{0}
      #mean flow rate, flows outside of range
      flow_mean <- round(raw_data[, mean(VolumetricFlowRate, na.rm=T)],3)
      flow_sd <- round(raw_data[, sd(VolumetricFlowRate, na.rm=T)], 3)
      flow_min <- raw_data[, min(VolumetricFlowRate, na.rm=T)]
      flow_max <- raw_data[, max(VolumetricFlowRate, na.rm=T)]
      percent_flow_deviation <- round(raw_data[!(VolumetricFlowRate %between% flow_thresholds), length(VolumetricFlowRate)]/raw_data[VolumetricFlowRate %between% flow_thresholds, length(VolumetricFlowRate)],2)
      flow_flag <- if(percent_flow_deviation>flow_cutoff_threshold | is.nan(percent_flow_deviation) | flow_min<flow_min_threshold | flow_max>flow_max_threshold){1}else{0}
      filename_flag <- filename$filename_flag
      
      #sample duration
      sample_duration <- raw_data_long[!is.na(value), as.numeric(difftime(max(datetime), min(datetime), units='days'))]
      datetime_start <- min(raw_data_long$datetime)
      datetime_end <- max(raw_data_long$datetime)
      gps_lat_med <- median(raw_data$GPSlat)
      gps_lon_med <- median(raw_data$GPSlon)
      if(meta_data$shutdown_reason=='1' | meta_data$shutdown_reason =='3'){shutdown_flag=0}else{shutdown_flag=1}
      
      meta_data = cbind(meta_data,gps_lat_med,gps_lon_med,flow_mean,flow_sd,flow_min,flow_max,percent_flow_deviation,flow_flag,inletp_flag,temp_flag,rh_flag,shutdown_flag,filename_flag)
      
      meta_data[, flag_total:=sum(flow_flag,inletp_flag,temp_flag,rh_flag,shutdown_flag,filename_flag), by=.SD]
      meta_data[, c('smry', 'analysis') := TRUE]
      
      return(meta_data)
    }
  }
}

