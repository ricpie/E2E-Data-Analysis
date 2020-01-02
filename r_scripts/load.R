if (!require("pacman")) install.packages("pacman")
pacman::p_load(lubridate,plyr,reshape2,devtools,shiny,shinydashboard,dygraphs,DT,shinyjs,tools,data.table,writexl,zoo,mailR,readxl
,gmailr,mailR,cronR,miniUI,shinyFiles,ggplot2,stringr,chron,doParallel,foreach,openxlsx,gridExtra)


todays_date <- gsub("-", "", as.character(Sys.Date()))
local_tz = "Africa/Nairobi"
equipment_IDs <- readRDS("Processed Data/equipment_IDs.R")
lascar_cali_coefs <- readRDS("Processed Data/lascar_calibration_coefs.R")
lascar_calibrated_timeseries <- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Calibrated Lascar.R")
tsi_timeseries <- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/tsi_timeseries.R")
beacon_data_timeseries<- readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Beacon_RawData.R")

cl <- makeCluster(detectCores()[1])
registerDoParallel(cl) 

source('thresholds-master/UPAS_thresholds.R')
source('thresholds-master/lascar_thresholds.R')
source('r_scripts/UPAS_functions.R')
source('r_scripts/lascar_functions.R')
source('r_scripts/pats_functions.R')
source('r_scripts/beacon_functions.R')
source('r_scripts/generic_functions.R')
source('r_scripts/tsi_functions.R')
