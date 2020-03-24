# fwrite(meta_data, file="r_scripts/pats_dummy_meta_data.csv") 
dummy_meta_data <- fread('r_scripts/pats_dummy_meta_data.csv')

pats_ingest <- function(file, output=c('raw_data', 'meta_data'), local_tz,preplacement,dummy='dummy_meta_data',meta='meta_emissions'){
  dummy_meta_data <- get(dummy, envir=.GlobalEnv)
  meta_emissions <- get(meta, envir=.GlobalEnv)
  
  badfileflag <- 0
  
  base::message(file, " QA-QC checking in progress")
  filename = tryCatch({
    filename <- parse_filename_fun(file)
  }, error = function(e) {
    print('error ingesting filename for ',file)
    filename$flag  = 'bad'
  })
  
  meta_data <- dummy_meta_data
  meta_data$file <- file
  meta_data$qa_date = as.Date(Sys.Date())
  meta_data$flags <- 'No usable data'
  suppressWarnings(meta_data[, c('smry', 'analysis') := NULL])
  
  #separate out raw, meta, and sensor data
  # raw_data_temp <- fread(file, skip = 30,sep=",",fill=TRUE)
  # if (nrow(raw_data_temp)<3 || ncol(raw_data_temp)<4 || filename$flag == 'bad'){
  #   badfileflag <- 1; slashpresence <- 1
  #   return(list(raw_data=NULL, meta_data=meta_data))
  # } else{
  raw_data <- fread(file, skip = 30,sep=",",fill=TRUE)
  
  if(badfileflag==1){
    base::message(basename(file), " has no parseable date.")
    #add "fake" meta_data
    meta_data <- dummy_meta_data
    meta_data$fullname = file
    meta_data$basename = basename(file)
    meta_data$qc <- 'bad'
    meta_data[, c('smry', 'analysis') := NULL]
    return(list(raw_data=NULL, meta_data=meta_data))
  }else{
    
    raw_data[, datetime:=ymd_hms(as.character(dateTime), tz=local_tz)]
    raw_data[,datetime := round_date(datetime, unit = "minutes")]
    
    #Sample rate. Time difference of samples, in minutes.
    sample_timediff = as.numeric(median(diff(raw_data$datetime)))/60
    
    #Sampling duration
    dur = difftime(max(raw_data$datetime),min(raw_data$datetime),units = 'days')
    
    meta_data <- data.table(
      fullname=filename$fullname,
      basename=filename$basename,
      qa_date = as.Date(Sys.Date()),
      datetime_start = raw_data$datetime[1],
      sampleID=filename$sampleID ,
      sampletype=filename$sampletype,
      fieldworkerID=filename$fieldworkerID, 
      fieldworkernum=filename$fieldworkernum, 
      HHID=as.integer(filename$HHID),
      loggerID=filename$loggerID,
      qc = filename$flag,
      samplerate_minutes = sample_timediff/60,
      sampling_duration = dur
    )
    
    #Add some meta_data into the mix
    raw_data <- as.data.table(raw_data)
    raw_data[,sampleID := meta_data$sampleID]
    raw_data[,loggerID := meta_data$loggerID]
    raw_data[,HHID := as.integer(meta_data$HHID)]
    raw_data[,sampletype := meta_data$sampletype]
    raw_data[,qc := meta_data$qc]
    raw_data <- tag_timeseries_mobenzi(raw_data,preplacement,filename)
    raw_data <- tag_timeseries_emissions(raw_data,meta_emissions,meta_data,filename)
    
    if(all(output=='meta_data')){return(meta_data)}else
      if(all(output=='raw_data')){return(raw_data)}else
        if(all(output == c('raw_data', 'meta_data'))){
          return(list(meta_data=meta_data, raw_data=raw_data))
        }
  }  
  # }
}

pats_qa_fun <- function(file,output= 'meta_data',local_tz="Africa/Nairobi",preplacement,pats_baseline_corrections='pats_baseline_corrections'){
  ingest = tryCatch({
    ingest <- suppressWarnings(pats_ingest(file, output=c('raw_data', 'meta_data'),local_tz,preplacement,pats_baseline_corrections))
  }, error = function(e) {
    print('error ingesting')
    ingest = NULL
  })
  
  if(is.null(ingest)){return(NULL)}else{
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data)){
      return(meta_data)
    }else{
      raw_data <- ingest$raw_data
      
      #create a rounded dt variable
      raw_data[, round_time:=round_date(datetime, 'hour')]
      
      #Filter the data based on actual start and stop times - once I get them!
      # raw_data <- raw_data[ecm_tags=='deployed']
      
      
      # Create a table with baseline parameters by hour
      # 5 percentile per hour, sd per hour, max per hour, num of observations per hour
      bl_check <- raw_data[!is.na(PM_Estimate), list(hr_ave=mean(PM_Estimate, na.rm=T),hr_p05=quantile(PM_Estimate, 0.05, na.rm=T), hr_sd=sd(PM_Estimate, na.rm=T), hr_n=length(PM_Estimate)), by='round_time']
      bl_check[, hour_of_day:=hour(round_time)]
      #check the first three values of the summary
      first_hrs_bl <- round(bl_check[, median(head(hr_p05,3))],2)
      #check the last three values of the summary
      last_hrs_bl <- round(bl_check[, median(tail(hr_p05,3))],2)
      #check the middle of night values of the summary
      night_hrs_bl <- round(bl_check[hour_of_day %in% c(23,0,1), median(hr_p05)],2)
      night_hrs_ave <- round(bl_check[hour_of_day %in% c(23,0,1), mean(hr_ave)],2)
      #calculate all diffs
      bl_diffs <- c((night_hrs_bl-first_hrs_bl), (last_hrs_bl-night_hrs_bl), (last_hrs_bl-first_hrs_bl))
      
      #Calculate baseline shift flag
      bl_flag <- if(any(abs(bl_diffs)>=10*bl_shift_threshold,na.rm = TRUE) || sum(is.na(bl_diffs)) > 0){1}else{0}
      
      #Calculate daily average concentration flag
      Daily_avg <- mean(raw_data$PM_Estimate,na.rm = TRUE)
      
      Daily_avg_flag <- if(Daily_avg>=10*Daily_avg_threshold || sum(is.na(raw_data$PM_Estimate)) > 0){1}else{0}
      Daily_sd <- sd(raw_data$PM_Estimate,na.rm = TRUE)
      
      #Calculate hourly average concentration flag
      max_1hr_avg <- max(bl_check$hr_ave,na.rm = TRUE)
      max_1hr_avg_flag <- if(max_1hr_avg>=50*max_1hr_avg_threshold || sum(is.na(bl_check$hr_ave)) > 0){1}else{0}
      
      #Raise flag if stdev of values is zero.
      nonresponsive_flag <- if(sd(raw_data$PM_Estimate,na.rm = TRUE)==0 || sum(is.na(raw_data$PM_Estimate)) > 0){1}else{0}
      
      #sample duration
      sample_duration_flag <- if(meta_data$sampling_duration < sample_duration_thresholds[1]/1440 &&
                                 ("A"==meta_data$sampletype || "C"==meta_data$sampletype ||  "K"==meta_data$sampletype)){1}else{0}
      
      meta_data = cbind(meta_data, night_hrs_bl,max_1hr_avg,Daily_avg,Daily_sd, max_1hr_avg_flag, Daily_avg_flag,nonresponsive_flag,sample_duration_flag,bl_flag)
      
      meta_data[, flag_total:=sum(max_1hr_avg_flag, Daily_avg_flag,nonresponsive_flag,sample_duration_flag,bl_flag), by=.SD]
      
      flags_str <- suppressWarnings(paste(melt(meta_data[,c(colnames(meta_data)[colnames(meta_data) %like% "flag"]), with=F])[value>0 & variable!="flag_total", gsub("_flag", "" , variable)] , collapse=", "))
      
      meta_data[, flags:=flags_str]
      raw_data <- as.data.table(raw_data)
      
      #Plot the PM data and save it
      tryCatch({ 
        #Prepare some text for looking at the ratios of high to low temps.
        plot_name = gsub(".txt",".png",basename(file))
        plot_name = paste0("QA Reports/Instrument Plots/pats_",gsub(".csv",".png",plot_name))
        percentiles <- quantile(raw_data$PM_Estimate,c(.05,.95))
        cat_string <- paste("5th % PM (ugm3) = ",as.character(percentiles[1]),
                            ", 95th % PM (ugm3)  = ",as.character(percentiles[2]))
        if(!file.exists(plot_name)){
          png(filename=plot_name,width = 550, height = 480, res = 100)
          plot(raw_data$datetime, raw_data$PM_Estimate, main=plot_name,
               type = "p", xlab = cat_string, ylab="Calibrated PM (ugm3)",prob=TRUE,cex.main = .6,cex = .5)
          grid(nx = 5, ny = 10, col = "lightgray", lty = "dotted",
               lwd = par("lwd"), equilogs = TRUE)
          dev.off()
        }
      }, error = function(error_condition) {
      }, finally={})
      
      #Plot the CO data and save it
      tryCatch({ 
        plot_name = gsub(".txt",".png",basename(file))
        plot_name = paste0("QA Reports/Instrument Plots/patsCO_",gsub(".csv",".png",plot_name))
        percentiles <- quantile(raw_data$CO_PPM,c(.05,.95))
        cat_string <- paste("5th % PM (ugm3) = ",as.character(percentiles[1]),
                            ", 95th % PM (ugm3)  = ",as.character(percentiles[2]))
        if(!file.exists(plot_name) & !is.na(percentiles[1]) & mean(raw_data$CO_PPM)>-1){
          png(filename=plot_name,width = 550, height = 480, res = 100)
          plot(raw_data$datetime, raw_data$CO_PPM, main=plot_name,
               type = "p", xlab = cat_string, ylab="Calibrated CO (ppm)",prob=TRUE,cex.main = .6,cex = .5)
          grid(nx = 5, ny = 10, col = "lightgray", lty = "dotted",
               lwd = par("lwd"), equilogs = TRUE)
          dev.off()
        }
      }, error = function(error_condition) {
      }, finally={})
      
      return(meta_data)
    }
  }
}

pats_import_fun <- function(file,output='raw_data',local_tz,preplacement,meta='meta_emissions',pats_baseline_corrections='pats_baseline_corrections'){
  meta_emissions <- get(meta, envir=.GlobalEnv)
  
  ingest <- suppressWarnings(pats_ingest(file, output=c('raw_data', 'meta_data'),local_tz="Africa/Nairobi",preplacement=preplacement))
  
  if(is.null(ingest)){return(NULL)}else{
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data)){
      return(meta_data)
    }else{
      
      raw_data <- as.data.table(ingest$raw_data)
      raw_data[,dateTime := NULL]
      raw_data[,V13:=NULL]
      raw_data[,V16:=NULL]
      raw_data[,V14:=NULL]
      raw_data[,iButton_Temp:=NULL]
      raw_data[,degC_sys:=NULL]
      raw_data[,CO_mV:=NULL]
      raw_data[,degC_CO:=NULL]
      return(raw_data)
    }
  }
}

