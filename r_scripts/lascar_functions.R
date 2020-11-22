# fwrite(meta_data, file="r_scripts/lascar_dummy_meta_data.csv") 
dummy_meta_data <- fread('r_scripts/lascar_dummy_meta_data.csv')

lascar_ingest <- function(file, output=c('raw_data', 'meta_data'), local_tz="Africa/Nairobi",dummy='dummy_meta_data',preplacement,meta='meta_emissions'){
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
  meta_data$qc <- filename$flag
  suppressWarnings(meta_data[, c('smry', 'analysis') := NULL])
  
  #separate out raw, meta, and sensor data
  raw_data_temp <- fread(file, skip = 0,sep=",",fill=TRUE)
  if (nrow(raw_data_temp)<3 || ncol(raw_data_temp)<4 || filename$flag == 'bad'){
    badfileflag <- 1; slashpresence <- 1
    return(list(raw_data=NULL, meta_data=meta_data))
  } else{
    
    #Handle whether the data is from a Lascar or PATS+
    raw_data <- fread(file, skip = 2,sep=",",fill=TRUE)
    if (raw_data[1,1]=="HW Version"){
      raw_data <- raw_data[30:dim(raw_data)[1],c(1,1,7)]
    } else {raw_data<- raw_data[,1:3]}
    setnames(raw_data, c("SampleNum", "datetime", "CO_raw"))
    raw_data<-raw_data[complete.cases(raw_data), ]
    raw_data$CO_raw = as.numeric(raw_data$CO_raw)
    if(mean(raw_data$CO_raw)<0){meta_data$qc = 'bad';filename$flag = 'bad'}

    # if(all(is.na(filename$sampleID))){
    #   #add "fake" meta_data
    # 
    # }else{
    
    #since samples are short, simply "guessing" doesn't work -- for example, some files were taken between 8/8 and 8/10.
    #Use a simple algorithm to determine, of the three DT options below, which one has the smallest overall range
    date_formats <- suppressWarnings(melt(data.table(
      dmy = raw_data[, as.numeric(difftime(max(dmy_hms(datetime)), min(dmy_hms(datetime)),units = "days"))],
      ymd = raw_data[, as.numeric(difftime(max(ymd_hms(datetime)), min(ymd_hms(datetime)),units = "days"))],
      mdy = raw_data[, as.numeric(difftime(max(mdy_hms(datetime)), min(mdy_hms(datetime)),units = "days"))]
    )))
    
    #Clunkily deal with this date format
    slashpresence <- unique((sapply(regmatches(raw_data[,datetime], gregexpr("/", raw_data[,datetime])), length)))>1
    
    #Some files don't have seconds in the timestamps... also clunky but oh well.
    semicolons <- unique((sapply(regmatches(raw_data[,datetime], gregexpr(":", raw_data[,datetime])), length)))<2
    if (semicolons==TRUE) {raw_data[, datetime:=paste0(datetime,":00")]}
    
    if(all(is.na(date_formats$value)) || badfileflag==1){
      base::message(basename(file), " has no parseable date.")
      #add "fake" meta_data
      meta_data <- dummy_meta_data
      meta_data$fullname = file
      meta_data$basename = basename(file)
      meta_data$qa_date = as.Date(Sys.Date())
      meta_data$flags <- 'No usable data'
      meta_data$qc <- "bad"
      meta_data[, c('smry', 'analysis') := NULL]
      return(list(raw_data=NULL, meta_data=meta_data))
    }else{
      
      # #deal with dates and times
      # raw_data[, datetime:=as.POSIXct(datetime)]
      
      date_format <- as.character(date_formats[value==date_formats[!is.na(value),min(value)], variable])
      
      if(slashpresence==1){raw_data[, datetime:=dmy_hms(as.character(datetime), tz=local_tz)]} else
        if(date_format=="mdy"){raw_data[, datetime:=mdy_hms(as.character(datetime), tz=local_tz)]} else
          if(date_format=="ymd"){raw_data[, datetime:=ymd_hms(as.character(datetime), tz=local_tz)]} else
            if(date_format=="dmy"){raw_data[, datetime:=dmy_hms(as.character(datetime), tz=local_tz)]}
      
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
        filterID=filename$filterID,
        sampletype=filename$sampletype,
        fieldworkerID=filename$fieldworkerID, 
        fieldworkernum=filename$fieldworkernum, 
        HHID=filename$HHID,
        loggerID=filename$loggerID,
        qc = filename$flag,
        samplerate_minutes = sample_timediff/60,
        sampling_duration = dur
      )
      
      #Add some meta_data into the mix
      raw_data[,datetime := floor_date(datetime, unit = "minutes")]
      raw_data[,sampleID := meta_data$sampleID]
      raw_data[,loggerID := meta_data$loggerID]
      raw_data[,HHID := meta_data$HHID]
      raw_data[,qc := meta_data$qc]
      raw_data[,sampletype := meta_data$sampletype]
      
      raw_data <- tag_timeseries_mobenzi(raw_data,preplacement,filename)
      #Tagging with emissions data because we want the Lascar data to calculate AERs.
      raw_data <- tag_timeseries_emissions(raw_data,meta_emissions,meta_data,filename)
      
      
      if(all(output=='meta_data')){return(meta_data)}else
        if(all(output=='raw_data')){return(raw_data)}else
          if(all(output == c('raw_data', 'meta_data'))){
            return(list(meta_data=meta_data, raw_data=raw_data))
          }
    }  
  }
}

lascar_qa_fun <- function(file, setShiny=TRUE,output= 'meta_data',local_tz="Africa/Nairobi",preplacement){
  ingest = tryCatch({
    ingest <- lascar_ingest(file, output=c('raw_data', 'meta_data'),local_tz="Africa/Nairobi",dummy='dummy_meta_data',preplacement=preplacement)
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
      calibrated_data <- as.data.table(apply_lascar_calibration(z,meta_data$loggerID,raw_data))
      
      #create a rounded dt variable
      calibrated_data[, round_time:=round_date(datetime, 'hour')]
      
      #Filter the data based on actual start and stop times - once I get them!
      # calibrated_data <- calibrated_data[ecm_tags=='deployed']
      
      # Create a table with baseline parameters by hour
      # 5 percentile per hour, sd per hour, max per hour, num of observations per hour
      bl_check <- calibrated_data[!is.na(CO_ppm), list(hr_ave=mean(CO_ppm, na.rm=T),hr_p05=quantile(CO_ppm, 0.05, na.rm=T), hr_sd=sd(CO_ppm, na.rm=T), hr_n=length(CO_ppm)), by='round_time']
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
      bl_flag <- if(any(abs(bl_diffs)>=bl_shift_threshold,na.rm = TRUE) || sum(is.na(bl_diffs)) > 0){1}else{0}
      
      #Calculate nighttime concentration flag
      elevated_night_flag <- if(any(abs(night_hrs_ave)>=elevated_night_threshold,na.rm = TRUE) || sum(is.na(night_hrs_ave)) > 0){1}else{0}
      
      #Calculate daily average concentration flag
      Daily_avg <- mean(calibrated_data$CO_ppm,na.rm = TRUE)
      Daily_avg_flag <- if(Daily_avg>=Daily_avg_threshold || sum(is.na(calibrated_data$CO_ppm)) > 0){1}else{0}
      Daily_sd <- sd(calibrated_data$CO_ppm,na.rm = TRUE)
      Daily_stdev_flag <- if(Daily_sd>=Daily_stdev_threshold || sum(is.na(calibrated_data$CO_ppm)) > 0){1}else{0}
      
      #Calculate hourly average concentration flag
      max_1hr_avg <- max(bl_check$hr_ave,na.rm = TRUE)
      max_1hr_avg_flag <- if(max_1hr_avg>=max_1hr_avg_threshold || sum(is.na(bl_check$hr_ave)) > 0){1}else{0}
      
      #Raise flag if stdev of values is zero.
      nonresponsive_flag <- if(sd(calibrated_data$CO_ppm,na.rm = TRUE)==0 || sum(is.na(calibrated_data$CO_ppm)) > 0){1}else{0}
      
      #sample duration
      sample_duration <- as.numeric(difftime(max(calibrated_data$datetime), min(calibrated_data$datetime), units='days'))
      sample_duration_flag <- if(sample_duration < sample_duration_thresholds[1]/1440){1}else{0}
      
      meta_data = cbind(meta_data, night_hrs_bl,max_1hr_avg,Daily_avg,Daily_sd, max_1hr_avg_flag, Daily_avg_flag,Daily_stdev_flag, elevated_night_flag,nonresponsive_flag,sample_duration_flag,bl_flag)
      
      meta_data[, flag_total:=sum(max_1hr_avg_flag, Daily_avg_flag, Daily_stdev_flag, elevated_night_flag,nonresponsive_flag,sample_duration_flag,bl_flag), by=.SD]
      
      flags_str <- suppressWarnings(paste(melt(meta_data[,c(colnames(meta_data)[colnames(meta_data) %like% "flag"]), with=F])[value>0 & variable!="flag_total", gsub("_flag", "" , variable)] , collapse=", "))
      
      meta_data[, flags:=flags_str]
      as.data.table(calibrated_data)
      
      #Plot the CO data and save it
      tryCatch({ 
        #Prepare some text for looking at the ratios of high to low temps.
        plot_name = gsub(".txt",".png",basename(file))
        plot_name = paste0("QA Reports/Instrument Plots/Lascar_",gsub(".csv",".png",plot_name))
        # plot_namez = paste0("QA Reports/Instrument Plots/Lascar_",gsub(".csv|.txt","_Zoom.png",basename(file)))
        percentiles <- quantile(calibrated_data$CO_ppm,c(.05,.95))
        cat_string <- paste("5th % CO (ppm) = ",as.character(percentiles[1]),
                            ", 95th % CO (ppm)  = ",as.character(percentiles[2]))
        if(!file.exists(plot_name)){
          png(filename=plot_name,width = 550, height = 480, res = 100)
          plot(calibrated_data$datetime, calibrated_data$CO_ppm, main=plot_name,
               type = "p", xlab = cat_string, ylab="Calibrated CO (ppm)",prob=TRUE,cex.main = .6,cex = .5)
          grid(nx = 5, ny = 10, col = "lightgray", lty = "dotted",
               lwd = par("lwd"), equilogs = TRUE)
          axis(3, calibrated_data$datetime, format(calibrated_data$datetime, "%b %d %y"), cex.axis = .7)
          
          dev.off()
          
          # png(filename=plot_namez,width = 550, height = 480, res = 100)
          # cookingstart = as.numeric(match("cooking",calibrated_data$emission_tags))
          # datasubset = calibrated_data[cookingstart:(cookingstart+200),]
          # plot(datasubset$datetime, datasubset$CO_ppm, main=plot_name,
          #      type = "p", xlab = cat_string, ylab="Calibrated CO (ppm)",prob=TRUE,cex.main = .6,cex = .5)
          # axis(3, datasubset$datetime, format(datasubset$datetime, "%b %d %y"), cex.axis = .7)
          # grid(nx = 5, ny = 10, col = "lightgray", lty = "dotted",
          #      lwd = par("lwd"), equilogs = TRUE)
          # dev.off()
          
        }
      }, error = function(error_condition) {
      }, finally={})
      
      return(meta_data)
    }
  }
}


lascar_cali_fun <- function(file,output='calibrated_data',local_tz="Africa/Nairobi",preplacement=preplacement){
  
  ingest = tryCatch({
    ingest <- lascar_ingest(file, output=c('raw_data', 'meta_data'),local_tz="Africa/Nairobi",dummy='dummy_meta_data',preplacement=preplacement)
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
      calibrated_data <- as.data.table(apply_lascar_calibration(file,meta_data$loggerID,raw_data)) 

      #Add a '2' if it is a PATS+.  Possible for there to be multiple duplicates.. K, K2, K22, etc.
      if(grepl("LAS|CAS",meta_data$loggerID)){ 
        calibrated_data[,sampletype := meta_data$sampletype]
      }else {calibrated_data[,sampletype := paste0(meta_data$sampletype,"2")]}
      
      calibrated_data[, sampletype := dt_case_when(sampletype == 'C2' ~ 'Cook Dup',
                                                            sampletype =='12' ~ '1m Dup',
                                                            sampletype == '22' ~ '2m Dup',
                                                            sampletype =='L22' ~ 'Living Room Dup',
                                                            sampletype == 'K22' ~ 'Kitchen Dup',           
                                                            TRUE ~ sampletype)]
      
      return(calibrated_data)
    }
  }
}

