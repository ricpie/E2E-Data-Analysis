dummy_meta_data$download_date <-  as.POSIXct(dummy_meta_data$download_date)
dummy_meta_data$file <-  as.character(dummy_meta_data$file)
dummy_meta_data$qa_date <-  as.Date(dummy_meta_data$qa_date)
dummy_meta_data$serial <-  as.character(dummy_meta_data$serial)
dummy_meta_data$pid <-  as.character(dummy_meta_data$pid)
dummy_meta_data$loc <-  as.character(dummy_meta_data$loc)
dummy_meta_data$study_phase <-  as.character(dummy_meta_data$study_phase)
dummy_meta_data$sampling_rate_minutes <-  as.character(dummy_meta_data$sampling_rate_minutes)
dummy_meta_data$dur <-  as.numeric(dummy_meta_data$dur)
dummy_meta_data$max_1hr_avg <-  as.numeric(dummy_meta_data$max_1hr_avg)
dummy_meta_data$max_1hr_avg_flag <-  as.numeric(dummy_meta_data$max_1hr_avg_flag)
dummy_meta_data$Daily_avg <-  as.numeric(dummy_meta_data$Daily_avg)
dummy_meta_data$Daily_stdev <-  as.numeric(dummy_meta_data$Daily_stdev)
dummy_meta_data$Daily_avg_flag <-  as.numeric(dummy_meta_data$Daily_avg_flag)
dummy_meta_data$Daily_sd <-  as.numeric(dummy_meta_data$Daily_sd)
dummy_meta_data$Daily_stdev_flag <-  as.numeric(dummy_meta_data$Daily_stdev)
dummy_meta_data$elevated_night_flag <-  as.numeric(dummy_meta_data$elevated_night_flag)
dummy_meta_data$nonresponsive_flag <-  as.numeric(dummy_meta_data$nonresponsive_flag)
dummy_meta_data$dur_flag <-  as.numeric(dummy_meta_data$dur_flag)
dummy_meta_data$flag_total <-  as.numeric(dummy_meta_data$flag_total)
dummy_meta_data$flags <- as.character(dummy_meta_data$flags)

###For debugging only
# tz="UTC"
# file <- "/Users/ricardopiedrahita/Box Sync/Exposure Data--INDIA/Main Trial/Nagapattinam_Main trial/Lascar_NP/Main Baseline/Processed file_NP/13088_PEM_1_LASC047_10122018.csv"
# file <- "~/Box Sync/Exposure Data--INDIA/Main Trial/Nagapattinam_Main trial/Lascar_NP/Main B1/Processed_NP/16001_KAP_4_LASC002_06022019.csv"
# file <- "/Users/ricardopiedrahita/Box Sync/Exposure Data--INDIA/Main Trial/Nagapattinam_Main trial/Lascar_NP/Main Baseline/Processed file_NP/16002_PEM_1_LASC006_04052018.csv"
# file <- "/Users/ricardopiedrahita/Box Sync/Exposure Data--INDIA/Main Trial/Nagapattinam_Main trial/Lascar_NP/Main Baseline/Processed file_NP/16025_PEM_1_LASC006_11062018.csv"
# file <- "/Users/ricardopiedrahita/Box Sync/Exposure Data--INDIA/Main Trial/Villupuram _Main trial/Lascar_VP/Main Baseline/Processed_VP/13085_PEM_1_LASC045_03122018.csv"
# file <- "~/Box Sync/Exposure Data--GUATEMALA/Main Trial/Instruments Data/Baseline/Lascar/33358_PEM_01_USB-CO-147_12042019.txt"
# file<- paste0(pathnames[[i]],"/23320_PEM_1_LAS11_18122018.txt")
# file<- paste0(pathnames[[i]],"/16002_PEM_1_LASC006_04052018.csv")
# file<- "/Users/ricardopiedrahita/Box Sync/Exposure Data--INDIA/Main Trial/Nagapattinam_Main trial/Lascar_NP/BL-P1/Processed NP/16043_KAP_S1_LASC019_15102018.csv"

# setShiny = TRUE
###

lascar_ingest <- function(file, tz="UTC", shiny=TRUE, output=c('raw_data', 'meta_data'), dummy='dummy_meta_data'){

  dummy_meta_data <- get(dummy, envir=.GlobalEnv)

  if(!shiny){lascar_copy(file)}
    
  base::message(basename(file), " QA-QC checking in progress")
  
  #separate out raw, meta, and sensor data
  raw_data_temp <- fread(file, skip = 0,sep=",",fill=TRUE)
    
  if (nrow(raw_data_temp)<3 || ncol(raw_data_temp)<4){badfileflag <- 1; slashpresence <- 1;date_formats$value = NA} else{
      raw_data <- fread(file, skip = 2,sep=",",fill=TRUE)
      badfileflag <- 0
      raw_data<- raw_data[,1:3]
      setnames(raw_data, c("SampleNum", "datetime", "CO_ppm"))
      raw_data<-raw_data[complete.cases(raw_data), ]
  
      #grab the lascar ID
      sensor_ID <- as.data.table(read.csv(file, skip=0, nrow=1, header=F, stringsAsFactor=F))[, c(1)]
  
      #since samples are short, simply "guessing" doesn't work -- for example, some files were taken between 8/8 and 8/10.
      #Use a simple algorithm to determine, of the three DT options below, which one has the smallest overall range
      date_formats <- suppressWarnings(melt(data.table(
        dmy = raw_data[, as.numeric(difftime(max(dmy_hms(datetime, tz=tz)), min(dmy_hms(datetime, tz=tz))))],
        ymd = raw_data[, as.numeric(difftime(max(ymd_hms(datetime, tz=tz)), min(ymd_hms(datetime, tz=tz))))],
  			mdy = raw_data[, as.numeric(difftime(max(mdy_hms(datetime, tz=tz)), min(mdy_hms(datetime, tz=tz))))]
      )))
    
      #Clunkily deal with this date format
      slashpresence <- unique((sapply(regmatches(raw_data[,datetime], gregexpr("/", raw_data[,datetime])), length)))>1
      
      #Some files don't have seconds in the timestamps... also clunky but oh well.
      semicolons <- unique((sapply(regmatches(raw_data[,datetime], gregexpr(":", raw_data[,datetime])), length)))<2
      if (semicolons==TRUE) {raw_data[, datetime:=paste0(datetime,":00")]}
    }
    
    if(all(is.na(date_formats$value)) || badfileflag==1){
    	base::message(basename(file), " has no parseable date.")
	    #add "fake" metadata
	    metadata <- dummy_meta_data
    	metadata$file <- basename(file)
		  metadata$qa_date = as.Date(Sys.Date())
		  metadata$flags <- 'No usable date'
		  metadata[, c('smry', 'analysis') := NULL]
	    return(list(raw_data=NULL, meta_data=metadata))
    }else{
      
      # #deal with dates and times
      # raw_data[, datetime:=as.POSIXct(datetime)]
      
	    date_format <- as.character(date_formats[value==date_formats[!is.na(value),min(value)], variable])

	    if(slashpresence==1){raw_data[, datetime:=dmy_hms(as.character(datetime, tz=tz))]} else
	    if(date_format=="mdy"){raw_data[, datetime:=mdy_hms(as.character(datetime, tz=tz))]} else
	    if(date_format=="ymd"){raw_data[, datetime:=ymd_hms(as.character(datetime, tz=tz))]} else
	    if(date_format=="dmy"){raw_data[, datetime:=dmy_hms(as.character(datetime, tz=tz))]}
	    
	    #Sample rate. Time difference of samples, in minutes.
	    sample_timediff = as.numeric(median(diff(raw_data$datetime)))
	    
	    #Sampling duration
	    dur = difftime(max(raw_data$datetime),min(raw_data$datetime))
	    
	    # download_date,file,qa_date,serial,pid,loc,
	    # study_phase,sampling_mode,dur,max_1hr_avg,Daily_avg,Daily_median,Daily_stdev,dur_flag,max_1hr_avg_flag,
	    # Daily_avg_flag,Daily_stdev_flag,elevated_night_flag,nonresponsive_flag,flag_total,flags
	    
	    meta_data <- data.table(
				download_date = NA,
				file = basename(file),
				qa_date = as.Date(Sys.Date()),
				serial = strsplit(basename(file), "_")[[1]][4],
				pid = strsplit(basename(file), "_")[[1]][1],
				loc = strsplit(basename(file), "_")[[1]][2],
				study_phase = strsplit(basename(file), "_")[[1]][3],
				sampling_rate_minutes = sample_timediff,
				dur = dur
	    )


	    if(all(output=='meta_data')){return(meta_data)}else
	    if(all(output=='raw_data')){return(raw_data)}else
	    if(all(output == c('raw_data', 'meta_data'))){
		    return(list(meta_data=meta_data, raw_data=raw_data))
	    }
	}  
}

lascar_qa <- function(file, setShiny=TRUE){
	ingest <- lascar_ingest(file, shiny=setShiny, output=c('raw_data', 'meta_data'))

	if(is.null(ingest)){return(NULL)}else{

		meta_data <- ingest$meta_data
		if('flags' %in% colnames(meta_data)){
			return(meta_data)
		}else{
			raw_data <- ingest$raw_data
			raw_data_long <- melt(raw_data[, c(2,3)], id.var='datetime')

			#create a rounded dt variable
			raw_data[, round_time:=round_date(datetime, 'hour')]

			#create a table with baseline parameters by hour
			# 5 percentile per hour, sd per hour, max per hour, num of observations per hour
			bl_check <- raw_data[!is.na(CO_ppm), list(hr_ave=mean(CO_ppm, na.rm=T),hr_p05=quantile(CO_ppm, 0.05, na.rm=T), hr_sd=sd(CO_ppm, na.rm=T), hr_n=length(CO_ppm)), by='round_time']
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
			#any diffs >= threshold = change in baseline
			
			#Calculate baseline shift flag
			bl_flag <- if(any(abs(bl_diffs)>=bl_shift_threshold,na.rm = TRUE) || sum(is.na(bl_diffs)) > 0){1}else{0}
      
			#Calculate nighttime concentration flag
			elevated_night_flag <- if(any(abs(night_hrs_ave)>=elevated_night_threshold,na.rm = TRUE) || sum(is.na(night_hrs_ave)) > 0){1}else{0}

			#Calculate daily average concentration flag
			Daily_avg <- mean(raw_data$CO_ppm,na.rm = TRUE)
			Daily_avg_flag <- if(Daily_avg>=Daily_avg_threshold || sum(is.na(raw_data$CO_ppm)) > 0){1}else{0}
			Daily_sd <- sd(raw_data$CO_ppm,na.rm = TRUE)
			Daily_stdev_flag <- if(Daily_sd>=Daily_stdev_threshold || sum(is.na(raw_data$CO_ppm)) > 0){1}else{0}

      #Calculate hourly average concentration flag
      max_1hr_avg <- max(bl_check$hr_ave,na.rm = TRUE)
      max_1hr_avg_flag <- if(max_1hr_avg>=max_1hr_avg_threshold || sum(is.na(bl_check$hr_ave)) > 0){1}else{0}
			
      #Raise flag if stdev of values is zero.
			nonresponsive_flag <- if(sd(raw_data$CO_ppm,na.rm = TRUE)==0 || sum(is.na(raw_data$CO_ppm)) > 0){1}else{0}

			#sample duration
			sample_duration <- raw_data_long[variable=='CO_ppm' & !is.na(value), as.numeric(difftime(max(datetime), min(datetime), units='mins'))]
			sample_duration_flag <- if(sample_duration < sample_duration_thresholds[1]){1}else{0}

			meta_data[, c("dur_flag", "bl_start", "bl_night", "bl_end", "bl_flag","max_1hr_avg","max_1hr_avg_flag","Daily_avg", "Daily_avg_flag","Daily_sd", "Daily_stdev_flag", "elevated_night_flag","nonresponsive_flag"):= list(sample_duration_flag, first_hrs_bl, night_hrs_bl, last_hrs_bl, bl_flag, max_1hr_avg, max_1hr_avg_flag, Daily_avg, Daily_avg_flag, Daily_sd, Daily_stdev_flag, elevated_night_flag,nonresponsive_flag )]
			
			meta_data[, flag_total:=sum(max_1hr_avg_flag, Daily_avg_flag, Daily_stdev_flag, elevated_night_flag,nonresponsive_flag,dur_flag,bl_flag), by=.SD]

			flags_str <- suppressWarnings(paste(melt(meta_data[,c(colnames(meta_data)[colnames(meta_data) %like% "flag"]), with=F])[value>0 & variable!="flag_total", gsub("_flag", "" , variable)] , collapse=", "))

			meta_data[, flags:=flags_str]

			return(meta_data)
		}
	}
}
