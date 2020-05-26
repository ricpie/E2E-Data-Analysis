# fwrite(meta_data, file="r_scripts/tsi_dummy_meta_data.csv") 
dummy_meta_data <- fread("r_scripts/tsi_dummy_meta_data.csv")


tsi_ingest <- function(file, local_tz, output=c('raw_data', 'meta_data'),dummy='dummy_meta_data',meta="meta_emissions"){
  dummy_meta_data <- get(dummy, envir=.GlobalEnv)
  meta_emissions <- get(meta, envir=.GlobalEnv)
  filename <- parse_filename_fun(file)
  badfileflag = 0
  #separate out raw, meta, and sensor data
  raw_data_temp <- fread(file, skip = 28,fill=TRUE)
  if (nrow(raw_data_temp)<10 && ncol(raw_data_temp)<6){
    badfileflag <- 1
  }else {
    raw_data <- fread(file, skip = 28,fill=TRUE,header = TRUE,check.names = TRUE)
    raw_data[, deg.C:=NULL]
    raw_data[, deg.C.1:=NULL]
    raw_data[, deg.C.2:=NULL]
    setnames(raw_data, c("Date",	"Time",	"CO2_ppm",	"RH","CO_ppm"))
    raw_data<-raw_data[complete.cases(raw_data), ]
  }
  
  if(all(is.na(filename$sampleID)) || badfileflag == 1){
    #add "fake" meta_data
    meta_data <- dummy_meta_data
    meta_data$file <- basename(file)
    meta_data$qa_date = as.Date(Sys.Date())
    meta_data$flags <- 'No usable data'
    meta_data$qc <- 'bad'
    meta_data[, c('smry', 'analysis') := NULL]
    return(list(raw_data=NULL, meta_data=meta_data))
  }else{
    
    raw_data$datetime <- paste(raw_data$Date,raw_data$Time)
    raw_data$datetime <- as.POSIXct(raw_data$datetime,tz=local_tz,tryFormats = c("%Y-%m-%d %H:%M:%OS","%m/%d/%Y %H:%M:%OS","%d/%m/%Y %H:%M:%OS"))
    if(raw_data$datetime[1]<1000){
      raw_data$datetime <- paste(raw_data$Date,raw_data$Time)
      raw_data$datetime <- as.POSIXct(raw_data$datetime,tz=local_tz,tryFormats = c("%m/%d/%y %H:%M:%OS","%d/%m/%Y %H:%M:%OS","%d/%m/%y %H:%M:%OS"))
    }
    #Shift forward time by 12 hours if the time was set up incorrectly.  Adjusting for one TSI in particular, model
    if(chron(times=raw_data$Time[1])<chron(times='07:00:00')){
      raw_data$datetime <- raw_data$datetime + 60*60*12
    }else if(chron(times=raw_data$Time[1])>chron(times='20:00:00')){
      raw_data$datetime <- raw_data$datetime - 60*60*12
    }

    raw_data[,datetime := floor_date(datetime, unit = "minutes")]
    
    #How many samples are invalid? If more than 10%, filename$flag = 'bad'
    fraction_invalid_CO2 = sum(raw_data$CO2_ppm %like% "Invalid")/length(raw_data$CO2_ppm)
    fraction_invalid_CO = sum(raw_data$CO_ppm %like% "Invalid")/length(raw_data$CO_ppm)
    if(fraction_invalid_CO2>0.1 | fraction_invalid_CO>0.1){filename$flag = 'bad'}
    
    raw_data$CO2_ppm[raw_data$CO2_ppm %like% "Invalid"] = 5000
    raw_data$CO2_ppm <- as.numeric(raw_data$CO2_ppm)
    raw_data$CO_ppm[raw_data$CO_ppm %like% "Invalid"] = 500
    raw_data$CO_ppm <- as.numeric(raw_data$CO_ppm)
    
    #Sample rate. Time difference of samples, in minutes.
    sample_timediff = as.numeric(median(diff(raw_data$datetime,units='minutes')))
    
    #Sampling duration
    dur = difftime(max(raw_data$datetime),min(raw_data$datetime),units='hours')
    
    meta_matched <- dplyr::left_join(filename,meta_emissions,by="HHID") %>% #in case of repeated households, keep the nearest one
      dplyr::filter(min(abs(difftime(datestart,datetimedecaystart,units='days')))==abs(difftime(datestart,datetimedecaystart,units='days')))
    
    meta_data <- data.table(
      fullname=filename$fullname,
      basename=filename$basename,
      datetime_start = raw_data$datetime[1],
      qa_date = as.Date(Sys.Date()),
      sampleID=filename$sampleID ,
      filterID=filename$filterID,
      sampletype=filename$sampletype, 
      stovetype=meta_matched$stovetype, 
      fieldworkerID=filename$fieldworkerID, 
      fieldworkernum=filename$fieldworkernum,
      HHID=filename$HHID,
      loggerID=filename$loggerID,
      qc = filename$flag,
      samplerate_minutes = sample_timediff,
      sampling_duration_hrs = dur
    )
    
    raw_data <- as.data.table(raw_data)
    raw_data[,stovetype := meta_data$stovetype]
    raw_data <- tag_timeseries_emissions(raw_data,meta_emissions,meta_data,filename)
    
  }
  if(all(output=='meta_data')){return(meta_data)}else
    if(all(output=='raw_data')){return(raw_data)}else
      if(all(output == c('raw_data', 'meta_data'))){
        return(list(meta_data=meta_data, raw_data=raw_data))
      }
}


tsi_qa_fun <- function(file,local_tz="Africa/Nairobi",output= 'meta_data',meta_emissions="meta_emissions"){
  ingest <- tsi_ingest(file,local_tz, output=c('raw_data', 'meta_data'),meta="meta_emissions")
  
  base::message(file, " QA-QC checking in progress")
  
  if(is.null(ingest) | dim(ingest$raw)[1]<5){return(NULL)}else{
    meta_data <- ingest$meta_data
    #Get emissions database row of interest
    
    if('flags' %in% colnames(meta_data) | dim(meta_data)[1]<1){
      return(meta_data)
    }else{
      
      raw_data <- ingest$raw_data
      
      #Separate chunks of the time series into the pre and post-sample background data, and the sample data.
      meta_data <- merge(meta_data,meta_emissions,by='HHID')   %>%
        dplyr::filter(min(abs(difftime(datetime_start,datetimedecaystart,units='days')))==abs(difftime(datetime_start,datetimedecaystart,units='days')))

      sampleperiod <-  subset(raw_data,datetime>meta_data$datetime_sample_start & datetime<meta_data$datetime_sample_end) #Emissions sample period
      BGi<- subset(raw_data,datetime<meta_data$datetime_sample_start) #initial background period
      BGf<- subset(raw_data,datetime>meta_data$datetimedecayend)#final background period. Playing it safer.
      # BGf<- subset(raw_data,datetime>meta_data$datetime_BGf_start)#final background period
      
      
      #check the initial and final background concentrations, based on start and stop times in the emissions database
      first_minutes_bl_CO2 <-  mean(BGi$CO2_ppm,na.rm = TRUE)
      last_minutes_bl_CO2 <- mean(BGf$CO2_ppm,na.rm = TRUE)
      first_minutes_bl_CO <- mean(BGi$CO_ppm,na.rm = TRUE)
      last_minutes_bl_CO <-  mean(BGf$CO_ppm,na.rm = TRUE)
      if(first_minutes_bl_CO>10 | is.na(first_minutes_bl_CO)){bg_start_flag=1}else{bg_start_flag=0} #Flag the initial background conc if the level is higher than 10ppm
      if(last_minutes_bl_CO>10 | is.na(last_minutes_bl_CO)){bg_end_flag=1}else{bg_end_flag=0} #Flag the final background conc if the level is higher than 10ppm
      
      #Raise flag that he instrument is non-responsive if stdev of values is zero.
      nonresponsive_flag <- if(sd(sampleperiod$CO_ppm,na.rm = TRUE) %in% 0 || sum(is.na(sampleperiod$CO_ppm)) > 0){1}else{0}
      
      #Raise flag if CO or CO2 max out.  Removed, since we are setting qc = 'bad' if more than 10% of values are invalid.
      # CO_maxout_flag <- if(sum(sampleperiod$CO_ppm==500)>2){1}else{0}
      # CO2_maxout_flag <- if(sum(sampleperiod$CO2_ppm==5000)>2){1}else{0}
      
      #Calculate background-subtracted average concentrations for the cooking event.
      Run_avg_CO <- mean(sampleperiod$CO_ppm,na.rm = TRUE)
      # meta_data$Run_avg_CO_BGS <-  Run_avg_CO-(mean(c(last_minutes_bl_CO,first_minutes_bl_CO),na.rm = TRUE))
      meta_data$Run_avg_CO_BGS <-  Run_avg_CO-first_minutes_bl_CO
      Run_sd_CO <- sd(sampleperiod$CO_ppm,na.rm = TRUE)
      Run_avg_CO2 <- mean(sampleperiod$CO2_ppm,na.rm = TRUE)
      # meta_data$Run_avg_CO2_BGS <- Run_avg_CO2-(mean(c(last_minutes_bl_CO2,first_minutes_bl_CO2),na.rm = TRUE))
      meta_data$Run_avg_CO2_BGS <- Run_avg_CO2-first_minutes_bl_CO2
      Run_sd_CO2 <- sd(sampleperiod$CO2_ppm,na.rm = TRUE)
      
      #Calculate emissions factors: g/kg fuel
      C_per_M3 <- 1/0.4905 # 1m3CO2/0.4905kg_C
      MM_CO2 <- 44.01 #g/mol
      MM_CO <- 28 #g/mol
      
      Mass_Conv <- 10^3 #mg/g
      R <- 0.08206 #Ideal gas constant: (𝑳. 𝒂𝒕𝒎)/(𝒎𝒐𝒍.𝑲)
      pressure_atm <- meta_data$`Pressure (hPa)`/1013.25
      ambient_temp_K <- meta_data$`Ambient Temp (C)` + 273
      Ultimate_Carbon_g <- meta_data$`Ultimate Carbon emissions (g)`
      Burn_Time <- as.numeric(difftime(meta_data$`Sample END time`,meta_data$`Sample START [hh:mm:ss]`,units = 'mins'))
      Ultimate_dryfuel_used_g <- meta_data$`Ultimate dry fuel used (g)`
      
      ppm_to_mgm3_function <- function(Conc_PPM_BGS,MolarMass,ambient_temp_K,pressure_atm,R,Mass_Conv){
        Vol_Conv <- 10^3 #L/m^3
        Mass_Conc_num <- Conc_PPM_BGS*10^-6*pressure_atm*MolarMass*Vol_Conv*Mass_Conv
        Mass_Conc_denom <- R*ambient_temp_K
        Mass_Conc <-Mass_Conc_num/Mass_Conc_denom
      }
      
      #Calculate the average CO concentration during the test on a mass basis
      meta_data$CO_mgm3 <- ppm_to_mgm3_function(meta_data$Run_avg_CO_BGS,MM_CO,ambient_temp_K,pressure_atm,R,Mass_Conv)
      
      #Kg wood on dry basis
      #ultimate? Try with the different assumptions. Will compare with MJ's Excel version.
      meta_data$EF_CO <- meta_data$CO_mgm3 * (1/(meta_data$Run_avg_CO_BGS+meta_data$Run_avg_CO2_BGS)) * C_per_M3*Mass_Conv *Ultimate_Carbon_g / Ultimate_dryfuel_used_g #[g_CO/kg_fuel]
      
      meta_data$ER_CO <- meta_data$EF_CO*Ultimate_dryfuel_used_g/Burn_Time/Mass_Conv #[g_CO/minute]
      
      meta_data$MCE <- meta_data$Run_avg_CO2_BGS/(meta_data$Run_avg_CO2_BGS+meta_data$Run_avg_CO_BGS)
      
      #Get the PM2.5 and PM2.5 mass concentrations in mg/m3 into the emissions spreadsheet to get these values
      # PM25_mgm3 <- meta_data$PM25_mgm3
      # PM25_BC_mgm3 <- meta_data$PM25_BC_mgm3
      # meta_data$EF_PM25 <- PM25_mgm3*(1/(meta_data$Run_avg_CO_BGS+meta_data$Run_avg_CO2_BGS))*C_per_M3*Mass_Conv *Ultimate_Carbon_g / Ultimate_dryfuel_used_g
      # meta_data$EF_PM25_BC <- PM25_BC_mgm3*(1/(meta_data$Run_avg_CO_BGS+meta_data$Run_avg_CO2_BGS))*C_per_M3*Mass_Conv *Ultimate_Carbon_g / Ultimate_dryfuel_used_g
      
      #Air exchange rate calculated from CO decay.
      raw_data_AER <- raw_data[raw_data$datetime<meta_data$datetimedecayend & raw_data$datetime>meta_data$datetimedecaystart,]
      meta_data <- AER_fun(file,meta_data,raw_data_AER,output = 'meta_data')
      raw_data[,BGdecay := 0]
      raw_data$BGdecay[raw_data$datetime<meta_data$datetimedecayend & raw_data$datetime>meta_data$datetimedecaystart] = 1 #Points that come after the decay start
      
      meta_data = cbind(meta_data, first_minutes_bl_CO,first_minutes_bl_CO2,last_minutes_bl_CO,last_minutes_bl_CO2,Run_avg_CO,Run_sd_CO,Run_avg_CO2,Run_sd_CO2, 
                        bg_start_flag,bg_end_flag,nonresponsive_flag) %>% as.data.table()
      
      meta_data[, flag_total:=sum(bg_start_flag,bg_end_flag,nonresponsive_flag), by=.SD]
      
      flags_str <- suppressWarnings(paste(melt(meta_data[,c(colnames(meta_data)[colnames(meta_data) %like% "flag"]), with=F])[value>0 & variable!="flag_total", gsub("_flag", "" , variable)] , collapse=", "))
      
      meta_data[, flags:=flags_str]
      
      tryCatch({ 
        #Prepare some text for looking at the ratios of high to low temps.
        plot_name = gsub(".csv",".png",basename(file))
        plot_name = paste0("QA Reports/Instrument Plots/TSI_",gsub(".CSV",".png",plot_name))
        tsiplot <- ggplot(raw_data,aes(y = CO_ppm, x = datetime,color=emission_tags)) +
          geom_line()+
          geom_point() +
          geom_line(data = raw_data, aes(y = CO2_ppm, x = datetime))+
          geom_point(data = raw_data, aes(y = CO2_ppm, x = datetime,color=emission_tags))+
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


tsi_meta_data_fun <- function(file,output='raw_data',local_tz,meta_emissions="meta_emissions"){
  print(file)
  ingest <- tsi_ingest(file,local_tz, output=c('raw_data', 'meta_data'),meta="meta_emissions")
  
  if(is.null(ingest)){return(NULL)}else{
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data)){
      return(meta_data)
    }else{
      raw_data <- ingest$raw_data
      
      #Add some meta_data into the mix
      raw_data[,sampleID := meta_data$sampleID]
      raw_data[,loggerID := meta_data$loggerID]
      raw_data[,HHID := meta_data$HHID]
      raw_data[,sampletype := meta_data$sampletype]
      raw_data[,qc := meta_data$qc]
      
      raw_data[,Date := NULL]
      raw_data[,Time := NULL]
      eturn(rarw_data)
    }
  }
}


