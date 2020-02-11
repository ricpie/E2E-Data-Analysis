
ecm_ingest <- function(file, output=c('raw_data', 'meta_data','preplacement'), local_tz,preplacement){
  # preplacement<-mobenzilist[[11]]
  
  base::message(file, " QA-QC checking in progress")
  filename = tryCatch({
    filename <- parse_filename_fun(file)
  }, error = function(e) {
    print('error ingesting filename for ',file)
    filename$flag  = 'bad'
  })
  
  #separate out raw, meta, and sensor data
  raw_data <- fread(file, skip = 0,sep=",",fill=TRUE)
  setnames(raw_data, c("date", "time", "pm2.5ugm3","TempC","RH","vectorcompGs"))
  
  raw_data[, datetime:=mdy_hms(as.character(paste(raw_data$date,raw_data$time,sep = " ")),tz =local_tz)]
  
  #Sample rate. Time difference of samples, in minutes.
  sample_timediff = as.numeric(median(diff(raw_data$datetime)))/60
  
  #Sampling duration
  dur = difftime(max(raw_data$datetime),min(raw_data$datetime),units = 'days')
  
  raw_data[,datetime := round_date(datetime, unit = "minutes")]
  raw_data[,date:=NULL]
  raw_data[,time:=NULL]
  raw_data[,TempC:=as.numeric(TempC)]
  
  #Wearing compliance
  active.minute.average = ddply(raw_data, .(datetime), summarise,vectorcompGs_SD = round(sd(vectorcompGs, na.rm=TRUE), digits = 3))
  active.minute.average$sd_composite_above_threshold = ifelse(active.minute.average$vectorcompGs_SD > accel_compliance_threshold, 1, 0) 
  raw_data<-raw_data[,list(vectorcompGs_min=mean(vectorcompGs,na.rm=TRUE),pm2.5ugm3=mean(pm2.5ugm3,na.rm=TRUE),
                 TempC=mean(TempC,na.rm=TRUE),RH=mean(RH,na.rm=TRUE)),by=datetime] #dataset averaged by minute
  
  raw_data$sd_composite_rollmean <- as.numeric(rollapply(active.minute.average$sd_composite_above_threshold, width=window_width,  FUN = mean, align = "center", na.rm = TRUE, fill = NA))  ## **** NOTE **** To change the width of the rolling mean window for compliance, change the parameter for "width" w.
  raw_data$compliant <- ifelse(raw_data$sd_composite_rollmean > 0, 1, 0) #Compliant samples
  
  
  meta_data <- data.table(
    fullname=filename$fullname,
    basename=filename$basename,
    qa_date = as.Date(Sys.Date()),
    datetime_start = raw_data$datetime[1],
    sampleID=filename$sampleID ,
    sampletype=filename$sampletype,
    fieldworkerID=filename$fieldworkerID, 
    fieldworkernum=filename$fieldworkernum, 
    HHID=filename$HHID,
    loggerID=filename$loggerID,
    filterID=filename$filterID,
    qc = filename$flag,
    samplerate_minutes = sample_timediff/60,
    sampling_duration = dur
  )
  
  #Add some meta_data into the mix
  raw_data[,sampleID := meta_data$sampleID]
  raw_data[,loggerID := meta_data$loggerID]
  raw_data[,HHID := meta_data$HHID]
  raw_data[,sampletype := meta_data$sampletype]
  raw_data[,qc := meta_data$qc]
  
  #Add tags to the data streams
  raw_data <- tag_timeseries_emissions(raw_data,meta_emissions,meta_data,filename)
  #Update this preplacement function when we have the ECM data!
  # preplacement <- update_preplacement(preplacement,raw_data)
  raw_data <- tag_timeseries_mobenzi(raw_data,preplacement,filename)
  
  #Filter the data based on actual start and stop times - once I get them!
  # raw_data <- raw_data[ecm_tags=='deployed']

  if(all(output=='meta_data')){return(meta_data)}else
    if(all(output=='raw_data')){return(raw_data)}else
      if(all(output == c('raw_data', 'meta_data','preplacement'))){
        return(list(meta_data=meta_data, raw_data=raw_data, preplacement=preplacement))
      }
}  


ecm_import_fun <- function(file,output='raw_data',local_tz,preplacement){
  ingest <- ecm_ingest(file, output=c('raw_data', 'meta_data','preplacement'),local_tz,preplacement)
  
  if(is.null(ingest)){return(NULL)}else{
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data)){
      return(meta_data)
    }else{
      raw_data <- ingest$raw_data
      
      return(raw_data)
    }
  }
}

  
