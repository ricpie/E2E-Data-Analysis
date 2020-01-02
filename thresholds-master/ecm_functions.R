dummy_meta_data$download_date <-  as.POSIXct(dummy_meta_data$download_date)
dummy_meta_data$file <-  as.character(dummy_meta_data$file)
dummy_meta_data$datetime_start <-  as.Date(dummy_meta_data$datetime_start)
dummy_meta_data$datetime_end <-  as.Date(dummy_meta_data$datetime_end)
dummy_meta_data$qa_date <-  as.Date(dummy_meta_data$qa_date)
dummy_meta_data$neph_slope <-  as.numeric(dummy_meta_data$neph_slope)
dummy_meta_data$neph_offset <-  as.integer(dummy_meta_data$neph_offset)           
dummy_meta_data$serial <-  as.character(dummy_meta_data$serial)
dummy_meta_data$hardware_dt <-  as.POSIXct(dummy_meta_data$hardware_dt)
dummy_meta_data$software_dt <-  as.POSIXct(dummy_meta_data$software_dt)
dummy_meta_data$software_vers <-  as.character(dummy_meta_data$software_vers)
dummy_meta_data$pid <-  as.character(dummy_meta_data$pid)
dummy_meta_data$loc  <-  as.character(dummy_meta_data$loc)
dummy_meta_data$study_phase <-  as.character(dummy_meta_data$study_phase)
dummy_meta_data$filter_id <-  as.character(dummy_meta_data$filter_id)
dummy_meta_data$sampling_mode <-  as.character(dummy_meta_data$sampling_mode)
dummy_meta_data$shutdown_reason <-  as.character(dummy_meta_data$shutdown_reason)
dummy_meta_data$dur <-  as.numeric(dummy_meta_data$dur)
dummy_meta_data$dur_flag <-  as.numeric(dummy_meta_data$dur_flag)
dummy_meta_data$flow_avg <-  as.numeric(dummy_meta_data$flow_avg)
dummy_meta_data$flow_sd <-  as.numeric(dummy_meta_data$flow_sd)
dummy_meta_data$flow_min <-  as.numeric(dummy_meta_data$flow_min)
dummy_meta_data$flow_max <-  as.numeric(dummy_meta_data$flow_max)
dummy_meta_data$n_flow_deviation <-  as.numeric(dummy_meta_data$n_flow_deviation)
dummy_meta_data$percent_flow_deviation <-  as.numeric(dummy_meta_data$percent_flow_deviation)
dummy_meta_data$flow_flag <-  as.numeric(dummy_meta_data$flow_flag)
dummy_meta_data$bl_start <-  as.numeric(dummy_meta_data$bl_start)
dummy_meta_data$bl_night <-  as.numeric(dummy_meta_data$bl_night)
dummy_meta_data$bl_end <-  as.numeric(dummy_meta_data$bl_end)
dummy_meta_data$bl_flag <-  as.numeric(dummy_meta_data$bl_flag)
dummy_meta_data$neph_mean <-  as.numeric(dummy_meta_data$neph_mean)
dummy_meta_data$neph_sd <-  as.numeric(dummy_meta_data$neph_sd)
dummy_meta_data$neph_min <-  as.numeric(dummy_meta_data$neph_min)
dummy_meta_data$neph_max <-  as.numeric(dummy_meta_data$neph_max)
dummy_meta_data$neph_n <-  as.numeric(dummy_meta_data$neph_n)
dummy_meta_data$neph_neg_n <-  as.numeric(dummy_meta_data$neph_neg_n)
dummy_meta_data$neph_percent_neg <-  as.numeric(dummy_meta_data$neph_percent_neg)
dummy_meta_data$median_neg_neph <-  as.numeric(dummy_meta_data$median_neg_neph)
dummy_meta_data$neph_flag <-  as.numeric(dummy_meta_data$neph_flag)
dummy_meta_data$flag_total <-  as.numeric(dummy_meta_data$flag_total)
dummy_meta_data$flags <- as.character(dummy_meta_data$flags)


ecm_ingest <- function(file, tz="UTC", shiny=TRUE, output=c('raw_data', 'meta_data'), dummy='dummy_meta_data'){

  dummy_meta_data <- get(dummy, envir=.GlobalEnv)

  if(!shiny){ecm_copy(file)}
  
  unprocessed <- read.csv(file, nrow=1, header=F, stringsAsFactor=F)$V1

  if(unprocessed=="?;m" || dim(fread(file, fill = TRUE))[1] < 30){
    base::message(basename(file), " is not processed. Process and try again.")
    rm(unprocessed)
    #add "fake" metadata
    metadata <- dummy_meta_data
    metadata$file <- basename(file)
	  metadata$qa_date = as.Date(Sys.Date())
	  metadata$flags <- 'file not processed'
	  metadata[, c('smry', 'analysis') := NULL]
    return(list(raw_data=NULL, meta_data=metadata))
  }else{
    rm(unprocessed)
    #separate out raw, meta, and sensor data
    raw_data <- fread(file, skip = 29) 
    
    #vector of column names from a "good" file
    full_columns_list <- c("Date", "Time", "RH-Corrected Nephelometer", "RH-Corrected Nephelometer HR", "Temp", "RH", "Battery", "Inlet Press", "Orifice Press", "Flow", "X-axis", "Y-axis", "Z-axis", "Vector Sum Composite", "ShutDownReason", "Wearing Compliance", "ValidityWearingCompliance validation")

    #columns are not necessarily in order, so we need to extract column names from the file to prevent errors
    #first, we find the row with the header - the first row with "Date,Time"
    header_id <- min(grep('Date,Time', as.character(read.delim(file, nrows=30, header=F, stringsAsFactor=F)$V1)))
    #next, we import that row
    header_names <- as.character(read.csv(file, skip=header_id,nrow=1, header=F, stringsAsFactor=F))
    
    #some files have a lot of extra columns, likely a byproduct of being opened in Excel and saved
    #others do not have enough columns, inexplicably. 
    #This tries to cope with that. 
    skipflag <- 0
    
    #Deals with another edge case that shifts the header row (e.g.~/Box Sync/ecm_test/12108_PEM_ECM123_060_25072017.csv")
    if(ncol(raw_data)>=17 & grepl(header_names[3],"μg/m³")){
			skipflag <- 1
			header_names <- as.character(fread(file, skip=header_id-skipflag, nrows=1, header=F))[1:17]
			raw_data <- raw_data[,c(1:17)]
			setnames(raw_data, header_names)
    }else if(ncol(raw_data)>=17){
		  raw_data <- raw_data[,c(1:17)]
		  setnames(raw_data, header_names)
    }else{
			setnames(raw_data, header_names)
			missing_cols <- full_columns_list[!(full_columns_list %in% header_names)]
			raw_data[, c(missing_cols) := NA]
    }

    setcolorder(raw_data, full_columns_list)

    setnames(raw_data, c('date', 'time', 'rh_cor_neph', 'rh_cor_neph_hr', 'temp', 'rh','batt',  'inlet_pres', 'orifice_pres', 'flow', 'acc_x',  'acc_y',  'acc_z',  'acc_comp', 'shutdown_reason',  'wearing_compliance', 'validity_wearing_comp'))

    raw_data[, flow:=as.numeric(flow)]

    meta_data <- as.data.table(read.csv(file, nrow=14-skipflag, header=F, stringsAsFactor=F))
	meta_data_dutycycle <- as.data.table(read.csv(file, skip = 11, nrows = 2, header = F, stringsAsFactors = F))

    #for the time being, we use precious little of this
    sensor_data <- as.data.table(read.csv(file, skip=13-skipflag, nrow=10, header=T, stringsAsFactor=F))[, c(1:6)]
    if (skipflag<1){sensor_data <- sensor_data[Sensor!='0.3']}
    setnames(sensor_data, c('sensor', 'slope', 'offset', 'log_interval', 'high_target', 'low_target'))

    #deal with dates and times
    raw_data[, datetime:=paste(date, time, sep=" ")]
    raw_data[, date:=NULL]
    raw_data[, time:=NULL]

    #since samples are short, simply "guessing" doesn't work -- for example, some files were taken between 8/8 and 8/10.
    #Use a simple algorithm to determine, of the three DT options below, which one has the smallest overall range
    date_formats <- suppressWarnings(melt(data.table(
			dmy = raw_data[, as.numeric(difftime(max(dmy_hms(datetime, tz=tz)), min(dmy_hms(datetime, tz=tz))))],
			ymd = raw_data[, as.numeric(difftime(max(ymd_hms(datetime, tz=tz)), min(ymd_hms(datetime, tz=tz))))],
			mdy = raw_data[, as.numeric(difftime(max(mdy_hms(datetime, tz=tz)), min(mdy_hms(datetime, tz=tz))))]
    )))

    if(all(is.na(date_formats$value))){
    	base::message(basename(file), " has no parseable date.")
	    #add "fake" metadata
	    metadata <- dummy_meta_data
    	metadata$file <- basename(file)
		  metadata$qa_date = as.Date(Sys.Date())
		  metadata$flags <- 'No usable date'
		  metadata[, c('smry', 'analysis') := NULL]
	    return(list(raw_data=NULL, meta_data=metadata))
    }else{

	    date_format <- as.character(date_formats[value==date_formats[!is.na(value),min(value)], variable])

	    if(date_format=="mdy"){raw_data[, datetime:=mdy_hms(datetime, tz=tz)]} else
	    if(date_format=="ymd"){raw_data[, datetime:=ymd_hms(datetime, tz=tz)]} else
	    if(date_format=="dmy"){raw_data[, datetime:=dmy_hms(datetime, tz=tz)]}

	    #remove columns we don't need
	    raw_data[, wearing_compliance:=NULL]
	    raw_data[, validity_wearing_comp:=NULL]

	    #find large breaks in data, remove everything before
	    #per JL's suggestion
	    drop_line_check <- raw_data[, diff(datetime)]>10
	    if(any(drop_line_check)==TRUE){
			drop_line <- min(which(drop_line_check))
		}

	    if(exists("drop_line")){raw_data <- raw_data[(drop_line+1):nrow(raw_data)]}

	    if (identical(meta_data[V1 %like% "v2.0", V1],character(0))){
			software_version <- "unknown_software_vers"
	    }else{
			software_version <- meta_data[V1 %like% "v2.0", V1]
	    }
	    
	    meta_data <- data.table(
				download_date = meta_data[V1=='Download Date:', mdy(V2, tz=tz)],
				file = basename(file),
				qa_date = as.Date(Sys.Date()),
				neph_slope = sensor_data[sensor=='Nephelometer', slope],
				neph_offset = sensor_data[sensor=='Nephelometer', offset],
				serial = meta_data[V1=='Device Serial #:', V2],
				hardware_dt = meta_data[V1=='Date/Time Hardware:', mdy(V2, tz=tz)],
				software_dt = meta_data[V1=='Date/Time Software:', dmy(V2, tz=tz)],
				software_vers = software_version,
				pid = strsplit(basename(file), "_")[[1]][1],
				loc = strsplit(basename(file), "_")[[1]][2],
				study_phase = strsplit(basename(file), "_")[[1]][3],
				filter_id = meta_data[V1=='Filter ID#:', V2],
				sampling_mode = meta_data[V1=='System Times', V2]
				#shutdown_reason = raw_data[shutdown_reason!='', paste(unique(shutdown_reason), collapse=",")]
				# shutdown_reason = tail(raw_data[!is.na(shutdown_reason) & shutdown_reason != '', shutdown_reason],1)
	    )

	    shutdown_reason_chk <- raw_data[shutdown_reason!='', paste(unique(shutdown_reason), collapse=",")]
	    if(grepl('Duration Stop', shutdown_reason_chk)){shutdown_reason_chk <- "Duration Stop"}
	    meta_data[, shutdown_reason:=shutdown_reason_chk]

	    if(all(output=='meta_data')){return(meta_data)}else
	    if(all(output=='raw_data')){return(raw_data)}else
	    if(all(output == c('raw_data', 'meta_data'))){
		    return(list(meta_data=meta_data, raw_data=raw_data))
	    }
	}    
  }
}

ecm_qa <- function(file, setShiny=TRUE){
	ingest <- ecm_ingest(file, shiny=setShiny, output=c('raw_data', 'meta_data'))

	if(is.null(ingest)){return(NULL)}else{

		meta_data <- ingest$meta_data
		if('flags' %in% colnames(meta_data)){
			return(meta_data)
		}else{
			raw_data <- ingest$raw_data
			raw_data_long <- melt(raw_data[, c(1,3,4,5,6,8,12,14)], id.var='datetime')

			#create a rounded dt variable
			raw_data[, round_time:=round_date(datetime, 'hour')]

			#create a table with baseline parameters by hour
			# 5 percentile per hour, sd per hour, max per hour, num of observations per hour
			bl_check <- raw_data[!is.na(rh_cor_neph), list(hr_p05=quantile(rh_cor_neph, 0.05, na.rm=T), hr_sd=sd(rh_cor_neph, na.rm=T), hr_max=max(rh_cor_neph, na.rm=T), hr_n=length(rh_cor_neph)), by='round_time']
			bl_check[, hour_of_day:=hour(round_time)]
			#check the first three values of the summary
			first_hrs_bl <- round(bl_check[, median(head(hr_p05,3))],2)
			#check the last three values of the summary
			last_hrs_bl <- round(bl_check[, median(tail(hr_p05,3))],2)
			#check the middle of night values of the summary
			night_hrs_bl <- round(bl_check[hour_of_day %in% c(23,0,1), median(hr_p05)],2)
			#calculate all diffs
			bl_diffs <- c((night_hrs_bl-first_hrs_bl), (last_hrs_bl-night_hrs_bl), (last_hrs_bl-first_hrs_bl))
			#any diffs >= 10 = change in baseline
			#RP added case for NA baselines.
			bl_flag <- if(any(abs(bl_diffs)>=bl_shift_threshold,na.rm = TRUE) || sum(is.na(bl_diffs)) > 0){1}else{0}

			#inlet pressure flag
			inletp_flag <- if(raw_data_long[variable=='inlet_pres' & !is.na(value), any(value>5)]){1}else{0}

			#temperature range flag
			temp_flag <- if(raw_data_long[variable=='temp' & !is.na(value), any(!(value %between% temp_thresholds))]){1}else{0}

			rh_flag <- if(!is.na(raw_data_long[variable=='rh' & !is.na(value), (mean(value)>rh_threshold)])&raw_data_long[variable=='rh' & !is.na(value), (mean(value)>rh_threshold)]){1}else{0}

			#negative neph values
			neph_mean <- round(raw_data[, mean(rh_cor_neph, na.rm=T)],0)
			neph_sd <- round(raw_data[, sd(rh_cor_neph, na.rm=T)], 0)
			neph_min <- round(raw_data[, min(rh_cor_neph, na.rm=T)],0)
			neph_max <- round(raw_data[, max(rh_cor_neph, na.rm=T)],0)
			length_neph <- nrow(raw_data_long[!is.na(value) & variable=='rh_cor_neph'])
			length_neph_neg <- nrow(raw_data_long[!is.na(value) & variable=='rh_cor_neph' & value<0])
			percent_neph_neg <- round(100*(length_neph_neg/length_neph),2)
			median_neg_neph <- raw_data_long[!is.na(value) & variable=='rh_cor_neph' & value<0, abs(median(value))]
			neph_flag <- if(is.na(percent_neph_neg)){1}else{if(percent_neph_neg>=neph_neg_threshold | neph_min<= (neph_neg_magnitude_threshold)){1}else{0}}

			#mean flow rate, flows outside of range
			flow_mean <- round(raw_data[, mean(flow, na.rm=T)],3)
			flow_sd <- round(raw_data[, sd(flow, na.rm=T)], 3)
			flow_min <- raw_data[, min(flow, na.rm=T)]
			flow_max <- raw_data[, max(flow, na.rm=T)]
			flow_missing_percent <- round(100*raw_data[is.na(flow), length(flow)]/nrow(raw_data),2)
			n_flow_deviation <- raw_data[!(flow %between% flow_thresholds), length(flow)]
			percent_flow_deviation <- round(raw_data[!(flow %between% flow_thresholds), length(flow)]/raw_data[flow %between% flow_thresholds, length(flow)],2)
			flow_flag <- if(percent_flow_deviation>flow_cutoff_threshold | is.nan(percent_flow_deviation) | flow_min<flow_min_threshold | flow_max>flow_max_threshold | raw_data[,any(shutdown_reason %like% "Flow blocked")] | flow_missing_percent == 100){1}else{0}

			#sample duration
			sample_duration <- raw_data_long[variable=='rh_cor_neph' & !is.na(value), as.numeric(difftime(max(datetime), min(datetime), units='mins'))]
			sample_duration_flag <- if(sample_duration %between% sample_duration_thresholds){0}else{1}
			datetime_start <- min(raw_data_long$datetime)
			datetime_end <- max(raw_data_long$datetime)
			
			#Wearing compliance
			raw_data$unique_min <- floor_date(raw_data$datetime, unit = "minute")
			
			active.minute.average = ddply(raw_data, .(unique_min), summarise, 
			                              # X.axis_SD = round(sd(acc_x, na.rm=TRUE), digits = 3),  #Only using composite acceleration atm, can add distinct axes if needed.
			                              Vector.Sum.Composite_SD = round(sd(acc_comp, na.rm=TRUE), digits = 3))
			
			active.minute.average$sd_composite_above_threshold = ifelse(active.minute.average$Vector.Sum.Composite_SD > accel_compliance_threshold, 1, 0) 
			
			active.minute.average$sd_composite_rollmean <- as.numeric(rollapply(active.minute.average$sd_composite_above_threshold, width=window_width,  FUN = mean, align = "center", na.rm = TRUE, fill = NA))  ## **** NOTE **** To change the width of the rolling mean window for compliance, change the parameter for "width" w.
			
			compliant_samples <- sum(ifelse(active.minute.average$sd_composite_rollmean > 0, 1, 0),na.rm=T) #Compliant samples
			samples <- sum(!is.na(active.minute.average$sd_composite_rollmean)) #Total non-NA samples contributing compliance info
			
			if (samples > 0) {
			  compliance_fraction = compliant_samples/samples
			} else {
			  compliance_fraction = 0
			}
			
			#If compliance fraction is low, and it is a PEM or PEO sample, then flag it.
			compliance_flag <- if(compliance_fraction<overall_compliance_threshold & meta_data$loc %in% c("PEM", "PEO")){1}else{0}
			
			meta_data[, c("dur","datetime_start","datetime_end", "dur_flag", "flow_avg", "flow_sd", 'flow_min', 'flow_max', 'n_flow_deviation', "percent_flow_deviation", "flow_flag", "bl_start", "bl_night", "bl_end", "bl_flag", 'neph_mean', 'neph_sd', 'neph_min', 'neph_max', "neph_n", "neph_neg_n", "neph_percent_neg", "median_neg_neph", "neph_flag", "inlet_pres_flag", 'temp_flag', 'rh_flag',"compliance_fraction","compliance_flag") := list(sample_duration, datetime_start, datetime_end, sample_duration_flag, flow_mean, flow_sd, flow_min, flow_max, n_flow_deviation, percent_flow_deviation, flow_flag, first_hrs_bl, night_hrs_bl, last_hrs_bl, bl_flag,neph_mean, neph_sd, neph_min, neph_max, length_neph, length_neph_neg, percent_neph_neg, median_neg_neph, neph_flag, inletp_flag, temp_flag, rh_flag,compliance_fraction,compliance_flag )]

			#Shutdown reason flag -- ++ sample_duration_flag
			meta_data[!(shutdown_reason %in% c('Duration Stop', 'Button Press')), dur_flag:=dur_flag+1]

			meta_data[, flag_total:=sum(dur_flag, neph_flag, flow_flag, bl_flag,compliance_flag), by=.SD]

			flags_str <- suppressWarnings(paste(melt(meta_data[,c(colnames(meta_data)[colnames(meta_data) %like% "flag"]), with=F])[value>0 & variable!="flag_total", gsub("_flag", "" , variable)] , collapse=", "))

			meta_data[, flags:=flags_str]

			return(meta_data)
		}
	}
}
