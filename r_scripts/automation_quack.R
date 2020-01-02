#!/usr/local/bin/Rscript

setwd('~/Box Sync/ecm_workers')

##### yes, I know. Feel free to fix it. 
suppressWarnings(suppressPackageStartupMessages(library(gmailr)))
suppressWarnings(suppressPackageStartupMessages(library(lubridate)))
suppressWarnings(suppressPackageStartupMessages(library(plyr)))
suppressWarnings(suppressPackageStartupMessages(library(ggplot2)))
suppressWarnings(suppressPackageStartupMessages(library(reshape2)))
suppressWarnings(suppressPackageStartupMessages(library(devtools)))
suppressWarnings(suppressPackageStartupMessages(library(zoo)))
suppressWarnings(suppressPackageStartupMessages(library(digest)))
suppressWarnings(suppressPackageStartupMessages(library(data.table)))
suppressWarnings(suppressPackageStartupMessages(library(parallel)))
suppressWarnings(suppressPackageStartupMessages(library(optparse)))

################# SET UP
Sys.setenv(tz = "UTC")

gm_auth_configure(path = "~/credentials.json")
gm_auth(email = "rfsfilterroom@gmail.com", cache = '~/.secret')

#source the dummy metadata file from github
dummy_meta_data <- fread('https://raw.githubusercontent.com/hapin-trial/thresholds/master/dummy_meta_data.csv')

#source the thresholds from github
suppressMessages(source_url("https://raw.githubusercontent.com/hapin-trial/thresholds/master/ecm_thresholds.R"))

#source the functions file from github
suppressMessages(source_url("https://raw.githubusercontent.com/hapin-trial/thresholds/master/ecm_functions.R"))

#### additional functions
createFileList <- function(x){
    import <- ecm_qa(x, setShiny=F)
    if(nrow(import)==0 || is.null(import)){
      # base::message(paste("File", basename(x), "contains no data.", sep=" "))
      NULL
    }else{
		import[, c("smry", "analysis") := list(FALSE, FALSE)]
    }
}

#backup ECM files, do not overwrite.
ecm_copy <- function(file){ 
  file.copy(file,'~/Box Sync/ecm_backup',overwrite = FALSE, recursive = FALSE,
            copy.mode = TRUE, copy.date = FALSE)
  try(file.copy(file,'~/ecm_backup_local',overwrite = FALSE, recursive = FALSE,
            copy.mode = TRUE, copy.date = FALSE))
}

generic_email_recipients <- "ajaypillarisetti@gmail.com,rpiedrahita@berkeleyair.com"

#### set up optparse
option_list = list(
	make_option(c('-c', '--country'), type = 'character', default = NULL, help = 'IRC Name (India, Peru, Rwanda, or Guatemala)', metavar = 'character')
	)

opt_parser = OptionParser(option_list = option_list)
opt = parse_args(opt_parser)
################# 

################# QUACK quack quack
#parse option from CLI
if (is.null(opt$country)){
	print_help(opt_parser)
	stop("At least one argument must be supplied (input country).n", call.=FALSE)
}else if(opt$country == 'Guatemala'){
	country_email_recipients <- "jiawen.liao@emory.edu,odeleon@CES.UVG.EDU.GT,emollinedo@CES.UVG.EDU.GT,jmccracken@ces.uvg.edu.gt"
	files_loc <- '~/Box Sync/Exposure Data--GUATEMALA/Main Trial/Instruments Data'
}else if(opt$country == 'India'){
	country_email_recipients <- "vigneswari@ehe.org.in,kalpanasrmc@ehe.org.in,thangavel@ehe.org.in"
	files_loc <- '~/Box Sync/Exposure Data--INDIA/Main Trial/'
}else if(opt$country == "Rwanda"){
	country_email_recipients <- "ephrem.d.hapin@gmail.com,miles.kirby@gmail.com"
	files_loc <- '~/Box Sync/Exposure Data--RWANDA/Raw/ECM/'
}else if(opt$country == "Peru"){
	country_email_recipients <- "vburrow1@jhmi.edu,underhill@jhu.edu,ecanuz@gmail.com"
	files_loc <- '/Users/ajay/Box Sync/Exposure Data--PERU--05202019/Exposure Data - PERU - Raw'
}

fileList_fn <- paste(tolower(opt$country), "_ecm_file_list.rds", sep="")
dateToken_fn <- paste(tolower(opt$country), "_date_token.rds", sep="")

#get a list of files
ecm_files <- list.files(files_loc, recursive=T, pattern='ECM|ecm|Ecm', full.names=T)
ecm_files <- grep('processed|Processed', ecm_files, value=T)
#special code to deal with some IRCs
if(opt$country == 'Rwanda'){ecm_files <- grep('processed|Processed', ecm_files, invert=F, value=T)}
if(opt$country != 'Rwanda'){ecm_files <- grep('RAW|raw|Raw|row|Row|ROW|raW', ecm_files, invert=T, value=T)}

#special code to deal with Peru
if(opt$country == 'Peru'){
	ecm_files <- list.files(files_loc, recursive=T, pattern='.csv|.CSV', full.names=T)
	ecm_files <- grep('Processed', ecm_files, value=T)
	ecm_files <- grep('ECM|ecm|Ecm', ecm_files, value=T)
}
#quick check and filtering of duplicates
dup_check <- data.table(path = ecm_files, fn = basename(ecm_files))
ecm_files <- dup_check[!duplicated(fn), path]

# check if IRC-specific summary file exists. If not, create. 
if(!file.exists(fileList_fn)){
	fileList <- mclapply(ecm_files, createFileList, mc.cores=12, mc.preschedule = FALSE)
	#exclude files that did not import
	fileList <- fileList[which(mapply(inherits, fileList, 'data.table'))]
	fileList <- rbindlist(fileList, fill = TRUE)
	fileList[!flags %in% c('file not processed', 'No usable date'), smry:=TRUE]
	saveRDS(fileList, fileList_fn)
}

#check if date token exists, if not, arbitrary date pre-HAPIN
if(file.exists(dateToken_fn)){
	dateToken <- readRDS(dateToken_fn)
}else{dateToken <- as.Date('2000-01-01', tz="UTC")}

#import the list of already QA'd files
fileList <- readRDS(fileList_fn)

#add newly uploaded files (files that are not in the fileList but are in Box)
toImport <- ecm_files[!basename(ecm_files) %in% fileList[,file]]

#hack to deal with India file
if(opt$country == 'India'){
	if(toImport == "/Users/ajay/Box Sync/Exposure Data--INDIA/Main Trial//Nagapattinam_Main trial/ECM_NP/Main P2/Processed_NP/18013_PEO_3_ECM301_1M50881_13022019.csv"){
		toImport  <- NULL
	}
}
if(length(toImport)>0){
	new_fileList <- try(mclapply(toImport, createFileList, mc.cores = 12))
	new_fileList <- new_fileList[which(mapply(inherits, new_fileList, 'data.table'))]
	new_fileList <- rbindlist(new_fileList, fill = TRUE)

	# if(class(new_fileList) == 'try-error'){
		# base::message("Cannot import.")
		# toImport <- NULL
	# }else if(class(new_fileList) == 'data.table') {
	if(nrow(new_fileList)>0){
		#only set files that processed properly to SMRY = TRUE
		new_fileList[!flags %in% c('file not processed', 'No usable date'), smry:=TRUE]
		#check to see if older summary files are lacking the datetime_start, datetime_end; add NA of appropriate class if so. 
		if(!'datetime_start' %in% colnames(fileList)){fileList[, datetime_start:=as.POSIXct(NA)]}
		if(!'datetime_end' %in% colnames(fileList)){fileList[, datetime_end:=as.POSIXct(NA)]}
		fileList <- rbind(fileList, new_fileList)
		saveRDS(fileList, fileList_fn)
	}else{base::message("No new data.")}
	# }
}

#only send files with flags after the last date the loop was run
# smry_for_email <- arrange(fileList[qa_date>dateToken], desc(download_date))
smry_for_email <- arrange(fileList, desc(download_date))

#files that need to be checked are with flags or those that are not summarized
files_to_check <- smry_for_email[flag_total>0 | smry==FALSE, unique(file)]

if(nrow(smry_for_email)>0){
	write.csv(smry_for_email, file=paste("~/Box Sync/ecm_email_summaries/", gsub("-","", Sys.Date()),".",tolower(opt$country), ".csv", sep=""), row.names=F)

	msg <- mime(To = paste(generic_email_recipients, if(exists("country_email_recipients")){country_email_recipients}else(NULL), sep=if(exists("country_email_recipients")){","}else{""}), From = "ajaypillarisetti@gmail.com", Subject = paste("[HAPIN]", Sys.Date(), "ECM Update"))

	tbl_of_errors <- as.data.table(table(smry_for_email[flags!="", flags]))[,paste(V1, N, sep=':\t\t', collapse='\n ')]

	message <- paste("Hi all! \n\nAttached is a summary of ", smry_for_email[,length(unique(file))], " files uploaded to dropbox between ", dateToken, " and ", Sys.Date(), ". Please check the attached csv file for more details on errors. If you open any of the data files with issues in Excel, DO NOT MAKE CHANGES and DO NOT SAVE the files. If you detect any issues, please check the ECMs. Remember, the code employed here is pretty sensitive; flagged files do not necessarily indicate data that cannot be used for subsequent analyses. \n\nBest, \nYour friendly HAPIN ECM Robot", sep="")

	msg  %>% attach_file(paste("~/Box Sync/ecm_email_summaries/", gsub("-","", Sys.Date()),".",tolower(opt$country), ".csv", sep="")) %>% attach_part(message) -> msg
	msg  %>% attach_file(paste("~/Box Sync/ecm_email_summaries/", gsub("-","", Sys.Date()),".",tolower(opt$country), ".csv", sep="")) -> msg1
}

if(length(toImport)>0 & nrow(smry_for_email)>0){
	send_message(msg1)
}

#save out Date Token
saveRDS(Sys.Date(), dateToken_fn)

#clear ws and quit
rm(list = ls())
quit(save = 'no')