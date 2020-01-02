
source('r_scripts/load.R')
source('r_scripts/LoggerShiftFunction.R')

###INSTRUCTIONS
#These scripts fix the timestamps on iButton data so that it may be ingested by sumsarized.  
#Corrected data is saved into a folder from where the files were selected.

#interactive file selection to fix time stamps and plot the data.
files_shift <- tk_choose.files(default = "", caption = "Select files",
                              multi = TRUE, filters = NULL, index = 1)
TimeZone <-readline(prompt="Enter the time zone data was collected in (e.g. -6 if Guatemala): ")
CorrectStartDate <-readline(prompt="Enter the correct local start date [yyyy-mm-dd]: ")
CorrectStartTime <-readline(prompt="Enter the correct local start time [HH:MM]: ")
#l_ply(files_shift, ShiftTimeStamp(files_shift,CorrectStartDate, CorrectStartTime,TimeZone) ,.progress='text') #Shift time stamps
sapply(files_shift, ShiftTimeStamp,CorrectStartDate, CorrectStartTime,TimeZone ) #Shift time stamps

filerun <- files_shift[1]


