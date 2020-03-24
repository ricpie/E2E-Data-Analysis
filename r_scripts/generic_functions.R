
parse_filename_fun <- function(file){
  tryCatch({ 
    filename = data.table()
    filename$fullname = gsub(' ','',file) 
    filename$fullname = gsub('.txt','',filename$fullname)
    filename$fullname = gsub('.csv','',filename$fullname)
    filename$datestart = ymd(strsplit(basename(filename$fullname), "_")[[1]][1])
    filename$basename = basename(filename$fullname)
    filename$basename_sansext = file_path_sans_ext(filename$basename)
    filename$sampleID = strsplit(filename$basename_sansext , "_")[[1]][2]
    filename$loggerID = strsplit(filename$basename_sansext , "_")[[1]][3]
    if(is.na(filename$loggerID)){filename$loggerID = 'Missing'}
    filename$filterID = strsplit(filename$basename_sansext , "_")[[1]][4] 
    filename$sampletype= strsplit(basename(filename$sampleID), "-")[[1]][3]
    filename$fieldworkerIDstr = strsplit(basename(filename$sampleID), "-")[[1]][2]
    matches <- regmatches(filename$fieldworkerIDstr, gregexpr("[[:digit:]]+", filename$fieldworkerIDstr))
    filename$fieldworkerID = as.numeric(matches)
    filename$HHIDstr = strsplit(basename(filename$sampleID), "-")[[1]][1]
    matches <- regmatches(filename$HHIDstr, gregexpr("[[:digit:]]+", filename$HHIDstr))
    filename$HHID =  as.numeric(matches)
    filename$flag = strsplit(filename$basename_sansext , "_")[[1]][5]
    if(is.na(filename$flag)) {filename$flag = c("good")}
    num_underscores <- lengths(regmatches(filename$basename, gregexpr("_", filename$basename)))
    if ( num_underscores > 3 |  (is.na(filename$filterID) & num_underscores>3) | is.na(filename$HHID) | filename$fieldworkerID > 25 |
        filename$fieldworkerID < 0) {filename$filename_flag = 1}else{filename$filename_flag = 0}
    filename
    return(filename)
  }, error = function(e) {
    print('Error parsing filename')
    print(file)
    return(NULL)
  })  
}


equipment_IDs_fun <- function(){
  # Import equipment ID info
  
  #separate out raw, meta, and sensor data
  equipmentIDpath<- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Metadata/E2E_BA equipment matrix_vF.xlsx"
  # equipmentIDs <- equipment_IDs_fun(equipmentIDpath)  
  equipment_IDs <- read_excel(equipmentIDpath, sheet = "Equipment IDs")[,c(2:4)]
  setnames(equipment_IDs,c("loggerID","BAID","instrument"))
  saveRDS(equipment_IDs,"Processed Data/equipment_IDs.rds")
  equipment_IDs
}


mobenzi_import_fun <- function(output=c('mobenzi_indepth', 'mobenzi_rapid','preplacement','postplacement')){
  fileindepth <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Mobenzi Data/In Depth/Responses.csv"
  filerapid <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Mobenzi Data/Rapid/Responses.csv"
  preplacementpath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Mobenzi Data/Pre Placement/Responses.csv"
  postplacementpath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Mobenzi Data/Post Placement/Responses.csv"
  
  mobenzi_indepth <- read.table(fileindepth, header=T,quote = "\"" ,sep=",",na.string=c("","null","NaN"),colClasses = "character")
  mobenzi_rapid <- read.table(filerapid, header=T,quote = "\"",sep=",",na.string=c("","null","NaN"),colClasses = "character")
  preplacement <- read.table(preplacementpath, header=T,quote = "\"",sep=",",na.string=c("","null","NaN"),colClasses = "character")[,1:89]
  preplacement <- preplacement[("Yes" == preplacement$A10 & !is.na(preplacement$A10)), ]
  
  setnames(preplacement,c("Submission_Id","Fieldworker_Name","Fieldworker_Id","Handset_Asset_Code","Handset_Identifier","Received","Start","End","Duration__seconds_","Latitude","Longitude","Language","Survey_Version","Modified_By","Modified_On","Complete","Worker","UNOPS_Date","UNOPS_HH","Something","Age","Gender","Childrenunder5YN","HealthConditions","OKtoWearHealthConditions","AwayFromHomeMoreThan5Hours","WillingToWearAwayFromHome","MicropemID","LascarID","UNOPSworkerPresent","OKLivingRoom","OkEmissions","EmissionsTime","UNOPSWorker","BeaconID1","BeaconID2","WalkThrough1Start","WalkThrough1End","WalkThrough2Start","WalkThrough2End","WalkThrough3Start","WalkThrough3End","HHID","WalkTimetoMainRoad","NoManufacturedStove","HasChimney","TraditionalStoveFAfrica/Nairobiures","FanStoveFAfrica/Nairobiures","KitchenLocation","MicropemIDKitchen","LascarIDKitchen","UNOPSyn","BeaconLoggerIDKitchen","IntensiveEquipmentYN","IntensiveYN","PATSorECMYN","ECMIDKitchen","PATSIDKitchen","ElectricYN","KeroseneYN","LPGYN","TraditionalYN","ManufacturedYN","TSFYN","OtherYN","OtherStoveType","MicropemDistanceCM","MicropemHeightCM","PictureYN","Number_Of_Children","ChildAge","ChildGender","Child2Age","Child2Gender","Child3Age","Child3Gender","ECMIDChild1","ECMIDChild2","ECMIDChild3","unknown","LivingRoomMonitoringYN","RoomTypeSecondary","WallTypeSecondary","PATSIDSecondary","LascarIDSecondary","BeaconLoggerIDSecondary","PATSDistanceFloorSecondaryCM","PicsSecondaryYN","DevicesONTime"))
  preplacement$HHIDstr = substring(preplacement$HHID, 
                                   1, sapply(preplacement$HHID, function(x) unlist(gregexpr('-',x,perl=TRUE))[1])-1)
  
  preplacement$start_datetime <- as.POSIXct(paste(preplacement$UNOPS_Date,preplacement$DevicesONTime,sep = " "),tz = "Africa/Nairobi",
                                            tryFormats = c("%d-%m-%Y %H:%M","%d-%m-%Y %H:%M:%OS","%d-%m-%y %H:%M","%d/%m/%Y %H:%M:%OS"))
  
  matches <- regmatches(preplacement$HHIDstr, gregexpr("[[:digit:]]+", preplacement$HHIDstr))
  preplacement$HHIDnumeric =  as.numeric(matches)
  postplacement <- read.table(postplacementpath, header=T,quote = "\"",sep=",",na.string=c("","null","NaN"),colClasses = "character")
  
  
  saveRDS(mobenzi_indepth,"Processed Data/mobenzi_indepth.rds")
  saveRDS(mobenzi_rapid,"Processed Data/mobenzi_rapid.rds")
  saveRDS(preplacement,"Processed Data/preplacement.rds")
  saveRDS(postplacement,"Processed Data/postplacement.rds")
  return(list(mobenzi_indepth=mobenzi_indepth, mobenzi_rapid=mobenzi_rapid,preplacement=preplacement,postplacement=postplacement))
}


# parse_mobenzi_fun <- function(preplacement,output = preplacement_meta){
#   preplacement_meta = list()
#   preplacement_meta$datestart = as.POSIXlt(preplacement$Start,tz="Africa/Nairobi","%d-%m-%Y")
#   preplacement_meta$beacon1ID = as.character(preplacement$)
#   preplacement_meta$beacon2ID = strsplit(basename(preplacement$basename), "_")[[1]][4] 
#   preplacement_meta$sampletype= strsplit(basename(preplacement$sampleID), "-")[[1]][3]
#   preplacement_meta$fieldworkerIDstr = strsplit(basename(preplacement_meta$sampleID), "-")[[1]][2]
#   matches <- regmatches(preplacement_meta$fieldworkerIDstr, gregexpr("[[:digit:]]+", preplacement_meta$fieldworkerIDstr))
#   preplacement_meta$fieldworkerID = as.numeric(matches)
#   preplacement_meta$HHIDstr = strsplit(basename(preplacement_meta$sampleID), "-")[[1]][1]
#   matches <- regmatches(preplacement_meta$HHIDstr, gregexpr("[[:digit:]]+", preplacement_meta$HHIDstr))
#   preplacement_meta$HHID =  as.numeric(matches)
#   preplacement_meta$sampleID = strsplit(basename(preplacement_meta$basename), "_")[[1]][2]
#   filename
# }


lascar_cali_import <- function(){
  lascarcalipath1 <- "~/Dropbox/UNOPS emissions exposure/Data/Calibrations/Lascar CO Calibration Template_8Aug_b_RP_trimmed.xlsx"
  lascarcalipath2 <- "~/Dropbox/UNOPS emissions exposure/Data/Calibrations/Lascar CO Calibration Template_8Aug_a_RP_trimmed.xlsx"
  lascar_cali_1 <- read_excel(lascarcalipath1,sheet = "CO cal Data",skip=4)[,c(2:5)]
  completerecords_1 <- na.omit(lascar_cali_1)
  lascar_cali_2 <- read_excel(lascarcalipath2,sheet = "CO cal Data",skip=4)[,c(2:5)]
  completerecords_2 <- na.omit(lascar_cali_2)
  
  lascar_cali_coefs = merge(completerecords_1,completerecords_2,all=TRUE) %>%
    dplyr::mutate(instrument = "Lascar") 
  setnames(lascar_cali_coefs,c("loggerID","COslope","R2","COzero","instument"))
  lascar_cali_coefs <- dplyr::mutate(lascar_cali_coefs,loggerID = paste0('LAS',loggerID)) %>%
    dplyr::mutate(loggerID = gsub("LASCAA","CAA",loggerID))
  
  saveRDS(lascar_cali_coefs,"Processed Data/lascar_calibration_coefs.rds")
  lascar_cali_coefs
}


apply_lascar_calibration<- function(file,loggerIDval,raw_data) {
  lascar_cali_coefs <- readRDS("Processed Data/lascar_calibration_coefs.rds")
  lascar_cali_coefs <- as.data.table(lascar_cali_coefs)
  logger_cali <- lascar_cali_coefs[loggerID=="loggerIDval",]
  if (!nrow(logger_cali)){
    logger_cali <- data.table(
      loggerID = NA,
      COslope = 1,
      COzero = 0,
      R2 = NA,
      instrument = NA)
  }
  calibrated_data <- as.data.table(dplyr::mutate(raw_data,CO_ppm = CO_raw*logger_cali$COslope + logger_cali$COzero)) %>%
    dplyr::select(-SampleNum,-CO_raw)
  calibrated_data
}

ambient_import_fun <- function(path_other,sheetname){
  meta_ambient <- read_excel(path_other,sheet = sheetname,skip=1)[,c(1:12)]
  setnames(meta_ambient,c('fieldworker_name','fieldworkerID','patsID','upasID','upas_filterID','lascarID','date_start','time_start','date_end','time_end','problems','comments'))
  meta_ambient <- meta_ambient[!is.na(meta_ambient$time_start),]
  meta_ambient$time_start <- strftime(meta_ambient$time_start, format="%H:%M:%S")
  meta_ambient$datetime_start <- as.POSIXct(paste(meta_ambient$date_start,meta_ambient$time_start,sep = " "),tz = "Africa/Nairobi")
  meta_ambient$time_end <- strftime(meta_ambient$time_end, format="%H:%M:%S",tz="Africa/Nairobi")
  meta_ambient$datetime_end <- as.POSIXct(paste(meta_ambient$date_end,meta_ambient$time_end,sep = " "),tz = "Africa/Nairobi")
  meta_ambient
}

emissions_import_fun <- function(path_emissions,sheetname,local_tz){
  meta_emissions <- read_excel(path_emissions,sheet = 'Data',skip=2)#[,c(1:12)]
  meta_emissions <- meta_emissions[!is.na(meta_emissions$`Date [m/d/y]`) & !is.na(meta_emissions$HH_ID)
                                   & !is.na(meta_emissions$`Sample START [hh:mm:ss]`),]
  meta_emissions$Date <- as.POSIXct(meta_emissions$`Date [m/d/y]`,tz=local_tz,tryFormats = c("%Y-%m-%d","%d-%m-%Y"))
  BG_initial_start_time <- strftime(meta_emissions$`Initial BG START [hh:mm:ss]`, format="%H:%M:%S",tz="UTC") #This is what it's coming in as.
  meta_emissions$datetime_BGi_start <- as.POSIXct(paste(meta_emissions$Date,BG_initial_start_time,sep = " "),tz = local_tz) #This is what we want it as.
  Sample_start_time <- strftime(meta_emissions$`Sample START [hh:mm:ss]`, format="%H:%M:%S",tz="UTC")
  meta_emissions$datetime_sample_start <- as.POSIXct(paste(meta_emissions$Date,Sample_start_time,sep = " "),tz = local_tz)
  Sample_end_time <- strftime(meta_emissions$`Sample END time`, format="%H:%M:%S",tz="UTC")
  meta_emissions$datetime_sample_end <- as.POSIXct(paste(meta_emissions$Date,Sample_end_time,sep = " "),tz = local_tz)
  BG_end_start_time <- strftime(meta_emissions$`Final Background start time`, format="%H:%M:%S",tz="UTC")
  meta_emissions$datetime_BGf_start <- as.POSIXct(paste(meta_emissions$Date,BG_end_start_time,sep = " "),tz = local_tz)
  datetimedecaystart <- strftime(as.character(meta_emissions$datetimedecaystart), format="%H:%M:%S",tz="UTC")
  meta_emissions$datetimedecaystart<-as.POSIXct(paste(meta_emissions$Date,datetimedecaystart,sep = " "),tz = local_tz)
  datetimedecayend <- strftime(as.character(meta_emissions$datetimedecayend), format="%H:%M:%S",tz="UTC")
  meta_emissions$datetimedecayend <- as.POSIXct(paste(meta_emissions$Date,datetimedecayend,sep = " "),tz = local_tz)
  
  matches <- regmatches(meta_emissions$HH_ID, gregexpr("[[:digit:]]+", meta_emissions$HH_ID))
  meta_emissions$HHID =  as.numeric(matches)
  meta_emissions
}

upas_record_import_fun <- function(path_other,sheetname){
  meta_filter <- read_excel(path_other,sheet = sheetname,skip=1)[,c(1:9)]
  setnames(meta_filter,c('upasID','HHID','fieldworkerID','upas_filterID','data_loaded','cartridgeID','sampletype','location_notes','notes'))
  meta_filter$data_loaded <- as.POSIXct(meta_filter$data_loaded,tz = "Africa/Nairobi")
  meta_filter
}

#a function to slice 5 digit of Beacon id, will add 0 in in front if less than n (n = 5) digits
id_slicer_add <- function(x, n){
  str2 = substr(x, nchar(x)-n+1, nchar(x))
  str2 = str_pad(str2,n,'left',pad = '0')
  return(str2)
}

#Convert ppm to mgm3
ppm_to_mgm3_function <- function(Conc_PPM_BGS,MolarMass,ambient_temp_K,pressure_atm,R,Mass_Conv){
  Vol_Conv <- 10^3 #L/m^3
  Mass_Conc_num <- Conc_PPM_BGS*10^-6*pressure_atm*MolarMass*Vol_Conv*Mass_Conv
  Mass_Conc_denom <- R*ambient_temp_K
  Mass_Conc <-Mass_Conc_num/Mass_Conc_denom
}


#Get AER.  Provide a segment of CO2 data, calculate and return the AER (the slope), correlation, and plot it.
AER_fun <- function(file,meta_data,raw_data_AER,output = 'meta_data'){
  
  raw_data_AER$time_hours <- (raw_data_AER$datetime-raw_data_AER$datetime[1])/3600 #Dif time in hours
  raw_data_AER$ln_CO2 <- log(raw_data_AER$CO2_ppm)
  meta_data$AERslope <- NA
  meta_data$AERr_squared <- NA
  
  #Plot data and save it
  meta_data <- tryCatch({ 
    lmfit <- lm(raw_data_AER$ln_CO2~raw_data_AER$time_hours)
    meta_data$AERslope <- lmfit$coefficients[2] 
    meta_data$AERr_squared <- summary(lmfit)$r.squared
    
    #Prepare some text for looking at the ratios of high to low temps.
    plot_name = gsub(".csv",".png",basename(file))
    plot_name = paste0("QA Reports/Instrument Plots/AER_",gsub(".csv",".png",plot_name))
    aerplot <- ggplot(raw_data_AER,aes(y = time_hours, x = ln_CO2)) +
      geom_point()+
      ggtitle(paste0('AER ',basename(file))) + 
      geom_smooth(method=lm) +
      theme(legend.title = element_blank())+
      theme_minimal() 
    print(aerplot)
    ggsave(filename=plot_name,plot=aerplot,width = 8, height = 6)
    return(meta_data)
  }, error = function(e) {
    print('Error calculating AER')
    return(meta_data)
  })
  
  return(meta_data)
}


ShiftTimeStamp_unops <- function(beacon_logger_data, newstartdatetime,TimeZone){
  
  #Number of seconds to shift the timestamps
  TimeShiftSeconds <- as.numeric(difftime(newstartdatetime, beacon_logger_data$datetime[1],units='secs'))
  
  #Shift the timestamps
  beacon_logger_data$datetime <- beacon_logger_data$datetime + TimeShiftSeconds
  beacon_logger_data
  #Get the timestamps in the right format.
  # beacon_logger_data[,datetime:=strftime(beacon_logger_data$datetime, "%Y-%m-%dT%H:%M:%S%Z", tz="GMT")]
  
}

ambient_timeseries <- function(CO_calibrated_timeseries,pats_data_timeseries){
  ambient_pats_data_timeseries <- pats_data_timeseries["A"==substr(sampletype,1,1),]
  ambient_data <- CO_calibrated_timeseries["A"==substr(sampletype,1,1),]
  # beacon_logger_COmerged = merge(beacon_logger_data,lascar_calibrated_subset, by.x = c("datetime","HHID","sampletype"),by.y = c("datetime","HHID","sampletype"), all.x = T, all.y = F)
  
  # ambient_data <- merge(ambient_data,ambient_pats_data_timeseries,by=c("datetime"),all.x = T, all.y = F)
  ambient_data <- melt(ambient_data, id.vars = c('datetime', 'sampletype','qc'), measure.vars = 'CO_ppm')
  meltedpats <- melt(pats_data_timeseries, id.vars = c('datetime', 'sampletype','qc'), measure.vars = c('PM_Estimate','degC_air','%RH_air','status'))
  ambient_data <- rbind(ambient_data,meltedpats)
  
}


#Summary of items required for a deployment, and the different sources.
# fwrite(deployment, file="r_scripts/dummy_deployment_data.csv") 

deployment_check_fun <-  function(preplacement,equipment_IDs,tz,local_tz,meta_emissions,beacon_meta_qaqc,tsi_meta,lascar_meta,upasmeta){
  
  dummy_deployment <- fread("r_scripts/dummy_deployment_data.csv")
  # preplacement<-mobenzilist[[1]]
  base::message(preplacement$HHID)
  preplacement$HHID <- preplacement$HHIDnumeric
  preplacement$datestart <- as.Date(preplacement$UNOPS_Date,format = "%d-%m-%Y")
  #Which date in mobenzi to use???  Not clear!!!  Was using this: preplacement$Start
  start_time <- as.POSIXct(strftime(paste(as.Date(preplacement$UNOPS_Date,format = "%d-%m-%Y"),preplacement$DevicesONTime),tz=local_tz),tz=local_tz)
  
  #Prep the metadatastreams for merging.
  upasmetaT <- upasmeta
  colnames(upasmetaT) <- paste("upas", colnames(upasmetaT), sep = "_")
  upasmetaT$HHID <- upasmetaT$upas_HHID
  upasmetaT$datestart <- upasmetaT$upas_datestart
  upasmetaT <- dplyr::filter(upasmetaT,difftime(upas_datetime_start,start_time,units='mins')<1440 &
                               difftime(upas_datetime_start,start_time,units='mins')>0) #Keep only near samples
  
  tsi_metaT <- tsi_meta
  colnames(tsi_metaT) <- paste("tsi", colnames(tsi_metaT), sep = "_")
  tsi_metaT$HHID <- tsi_metaT$tsi_HHID
  tsi_metaT <- dplyr::filter(tsi_metaT,difftime(tsi_datetime_start,start_time,units='mins')<1440 &
                               difftime(tsi_datetime_start,start_time,units='mins')>0) #Keep only near samples that started after the personal sampling starttime
  lascar_metaT <- lascar_meta
  colnames(lascar_metaT) <- paste("lascar", colnames(lascar_metaT), sep = "_")
  lascar_metaT$HHID <- lascar_metaT$lascar_HHID
  lascar_metaT$sampletype <- lascar_metaT$lascar_sampletype
  lascar_metaT$datestart <- lascar_metaT$lascar_datestart
  lascar_metaT <- dplyr::filter(lascar_metaT,difftime(lascar_datetime_start,start_time,units='mins')>-1440 &
                                  difftime(lascar_metaT$lascar_datetime_start,start_time,units='mins')<1440) #Keep only near samples that started before the personal sampling starttime
  
  beacon_meta_qaqcT <- beacon_meta_qaqc
  colnames(beacon_meta_qaqcT) <- paste("beacon", colnames(beacon_meta_qaqcT), sep = "_")
  beacon_meta_qaqcT$HHID <- beacon_meta_qaqcT$beacon_HHID
  beacon_meta_qaqcT$sampletype <- beacon_meta_qaqcT$beacon_sampletype
  beacon_meta_qaqcT$datestart <- beacon_meta_qaqcT$beacon_datestart
  
  meta_emissionsT <- meta_emissions
  colnames(meta_emissionsT) <- paste("emissions", colnames(meta_emissionsT), sep = "_")
  meta_emissionsT$HHID <- meta_emissionsT$emissions_HHID
  meta_emissionsT$datestart <- meta_emissionsT$emissions_datetime_sample_start
  meta_emissionsT <- dplyr::filter(meta_emissionsT,abs(difftime(meta_emissionsT$emissions_datetimedecaystart,start_time,units='mins'))<1440) #Keep only near samples
  
  #Build deployment summaries
  deployment = tryCatch({ 
    
    #Merge streams
    upasqa_subset = merge(preplacement,upasmetaT,by='HHID')
    TSI_subset = merge(preplacement,tsi_metaT,by='HHID')
    Lascar_subset = merge(preplacement,lascar_metaT,by=c('HHID'))
    Beacon_subset = merge(preplacement,beacon_meta_qaqcT,by=c('HHID'))
    Emissions_subset = merge(preplacement,meta_emissionsT,by=c('HHID'))
    
    
    #Add flag summaries at some point!
    #Only grab the rows with the L and K sampletypes.
    Beacon_subsetK <- dplyr::filter(Beacon_subset,sampletype=='K')
    Beacon_subsetL <- dplyr::filter(Beacon_subset,sampletype=='L')
    Lascar_subsetK <- dplyr::filter(Lascar_subset,sampletype=='K')
    Lascar_subsetL <- dplyr::filter(Lascar_subset,sampletype=='L')
    
    deployment = data.frame(HHID = preplacement$HHID[1], 
                            startimemob = start_time)
    
    deployment$startdate_tsi = TSI_subset$tsi_datetime_start[1]
    deployment$tsiFile = TSI_subset$tsi_basename[1]
    deployment$BeaconLoggerKmob = preplacement$BeaconLoggerIDKitchen[1]
    deployment$BeaconLoggerKFile =  Beacon_subsetK$beacon_basename[1]
    deployment$BeaconLoggerLmob = preplacement$BeaconLoggerIDSecondary[1]
    deployment$BeaconLoggerLFile =  Beacon_subsetL$beacon_basename[1]
    deployment$PATSKmob = preplacement$PATSIDKitchen[1]
    deployment$PATSKEmXLSX = Emissions_subset$`emissions_PATS+ ID 1.5m`[1]
    # PATSKFile = 
    deployment$PATSLmob = preplacement$PATSIDSecondary[1]
    # PATSLFile = ,
    deployment$lascarKmob = preplacement$LascarIDKitchen[1]
    deployment$lascarKFile = Lascar_subsetK$lascar_basename[1]
    deployment$lascarKEmXLSX = Emissions_subset$`emissions_LASCAR ID 1.5m`[1]
    deployment$lascarLmob = preplacement$LascarIDSecondary[1]
    deployment$lascarLFile =  Lascar_subsetL$lascar_basename[1]
    deployment$upasIDFile = upasqa_subset$upas_basename[1]
    deployment$upasFilterFile = upasqa_subset$upas_filterID[1]
    deployment$upasFilterEmXLSX = Emissions_subset$upas_filterID[1]
    
    
    #If there are other sample types, manage them here, adding to the deployment dataframe.
    upasTSILascar_subsetC <- dplyr::filter(Lascar_subset,sampletype=='C')
    deployment$lascarCFile = upasTSILascar_subsetC$lascar_basename[1]
    deployment$lascarCmob = upasTSILascar_subsetC$lascar_loggerID[1]
    
    upasTSILascar_subset1 <- dplyr::filter(Lascar_subset,sampletype=='1')
    deployment$lascar1File = upasTSILascar_subset1$lascar_basename[1]
    deployment$lascar1EmXLSX = Emissions_subset$`emissions_LASCAR ID 1m`[1]
    
    upasTSILascar_subset2 <- dplyr::filter(Lascar_subset,sampletype=='2')
    deployment$lascar2File = upasTSILascar_subset2$lascar_basename[1]
    deployment$lascar2EmXLSX = Emissions_subset$`emissions_Lascar ID 2m`[1]
    deployment
  }, error = function(error_condition) {
    #add "fake" meta_data
    base::message("Error building deployment")
    deployment <- dummy_deployment
    deployment$HHID <- preplacement$HHID
    deployment$datetime_start <- start_time
    deployment})
}



emailgroup <-  function(todays_date){
  sender <- "beaconnih@gmail.com"
  recipients <- c("rpiedrahita@berkeleyair.com","mrossanese@berkeleyair.com","sdelapena@berkeleyair.com")
  send.mail(from = sender,
            to = recipients,
            subject = paste0("UNOPS/CAA QA Report ",as.character(Sys.Date())),
            body = "UNOPS/CAA data report (new format). Please correct filenames as needed, and note any devices that need maintenance. 
          The Deployment Summary worksheet has information from the filenames, Mobenzi, and Excel emissions database.  These should be cross-referenced and disparities investigated!",
            smtp = list(host.name = "smtp.gmail.com", port = 465, 
                        user.name = "beaconnih@gmail.com",            
                        passwd = "CookaBLE99", ssl = TRUE),
            authenticate = TRUE,
            attach.files = c(paste0("QA Reports/QA report", "_", todays_date, ".xlsx")),
            send = TRUE)
}

#Not done - need to truncate the ends.
truncate_timeseries <- function(raw_data,startdatetime,enddatetime){
  raw_data <- raw_data[datetime>startdatetime,]# & datetime<enddatetime)
}


#Add tag to data based on mobenzi info
#Good.
tag_timeseries_mobenzi <- function(raw_data,preplacement,filename){
  #Get the relevant preplacement row, based on HHID and start date.
  raw_data[,ecm_tags:="collecting"]
  
  if(raw_data$sampletype[1] %in% c('C','L','K','C2','L2','K2')){
    preplacement$HHID <- preplacement$HHIDnumeric
    preplacement_matched <- merge(raw_data[1,],preplacement, by="HHID") %>%
      dplyr::filter(datetime-start_datetime<1)
    ECM_end = preplacement_matched$start_datetime+86400
      
    if(dim(preplacement_matched)[1]>0){ #If there is a match
      raw_data[,ecm_tags := ifelse(datetime>preplacement_matched$start_datetime & datetime<ECM_end,'deployed',ecm_tags)]
      if(abs(raw_data$datetime[1]-max(raw_data$datetime))>2){      
        raw_data[,ecm_tags := ifelse(datetime > ECM_end,'intensive',ecm_tags)]
      }
    } else {
      print(paste('Error mobenzi-tagging ', filename$basename))
      }
  }
  raw_data
}


#Add tag to data based on emissions database info
tag_timeseries_emissions <- function(raw_data,meta_emissions,meta_data,filename){
  #Get the relevant preplacement row, based on HHID and start date.
  raw_data[,emission_tags:="collecting"]
  
  meta_matched <- dplyr::left_join(meta_data,meta_emissions,by="HHID") %>% #in case of repeated households, keep the nearest one
    dplyr::filter(min(abs(difftime(datetime_start,datetimedecaystart,units='days')))==abs(difftime(datetime_start,datetimedecaystart,units='days')))
  
  if(dim(meta_matched)[1]>0){
    raw_data[,'emission_tags'][raw_data$datetime %between% c(meta_matched$datetime_sample_start,meta_matched$datetime_BGf_start)] ="cooking"
    raw_data[,'emission_tags'][raw_data$datetime %between% c(meta_matched$datetime_BGi_start,meta_matched$datetime_sample_start) ] ="BG_initial"
    raw_data[,'emission_tags'][raw_data$datetime %between% c(meta_matched$datetime_sample_end,meta_matched$datetime_BGf_start)] ="BG_final"
    raw_data[,'emission_tags'][raw_data$datetime %between% c(meta_matched$datetimedecaystart,meta_matched$datetimedecayend)] ="Decay"

  } else if(raw_data$sampletype[1] %in% 'A'){
    print(paste('Ambient file found ', filename$basename))
  } else {
    print(paste('Error emissions-tagging, no meta_data matched ', filename$basename))
  }
  return(raw_data)
}


#Get start and stop times of ECM files to use in the deployments
update_preplacement <- function(raw_data,preplacement){
  #If it is a kitchen or cook's ECM
  if(raw_data$sampletype[1] %in% c('C','K','K2')){
    raw_data_temp <- raw_data[c(1,.N),.(datetime,HHID)]
    raw_data_temp$UNOPS_Date <- as.Date(raw_data_temp$datetime, "%Y-%m-%d")
    raw_data_temp$UNOPS_Date <-format(raw_data_temp$UNOPS_Date, "%d-%m-%Y")   
    
    #Get index of matching preplacement row
    preplacement_test <- merge(preplacement,raw_data_temp[1,], by=c("HHID","UNOPS_Date")) %>%
      dplyr::filter(datetime-start_datetime<1)
    preplacement$ECM_start_k
    preplacement_test
    
    
    
  }
}




