library(plyr)
library(lubridate)
library(tools)
library(data.table)

ShiftTimeStamp <- function(filerun, CorrectStartDate ,CorrectStartTime,TimeZone){
  #Time/date to shift start to
  CorrectStartTimeStamp <- as.POSIXct(paste(CorrectStartDate,CorrectStartTime,sep=" ")) #Local time
  CorrectStartTimeStamp = CorrectStartTimeStamp - as.double(TimeZone)*60*60 #GMT/UTC time
  
  #Extract the header
  header <- read.csv(filerun, nrow=1, blank.lines.skip=F, header=F, stringsAsFactor=F)
     
  beacon_logger_data <- as.data.table(read_csv(filerun,
                                 skip=1,col_names =c('datetime','MAC','RSSI'),col_types = "ccd"))
  beacon_logger_data$datetime <- as.POSIXct(beacon_logger_data$datetime, "%Y-%m-%dT%H:%M:%S", tz="UTC")
  beacon_logger_data$MAC <- as.factor(beacon_logger_data$MAC)
  
  #Number of seconds to shift the timestamps
  TimeShiftSeconds <- as.numeric(difftime(CorrectStartTimeStamp, beacon_logger_data$datetime[1],units='secs'))
  
  #Shift the timestamps
  beacon_logger_data$datetime <- beacon_logger_data$datetime + TimeShiftSeconds
  
  #Get the timestamps in the right format.
  beacon_logger_data[,datetime:=strftime(beacon_logger_data$datetime, "%Y-%m-%dT%H:%M:%S%Z", tz="GMT")]
  
}

