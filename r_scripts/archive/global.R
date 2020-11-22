library(lubridate); library(plyr); library(reshape2); library(devtools); library(shiny); library(shinydashboard); library(dygraphs); library(DT); library(shinyjs); library(tools); library(data.table); library(writexl); library(zoo)

#source dummy from github
dummy_meta_data <- fread('https://raw.githubusercontent.com/hapin-trial/thresholds/master/dummy_meta_data.csv')

#source dummy locally
# dummy_meta_data <- fread('dummy_meta_data.csv')

#source the thresholds from github
source_url("https://raw.githubusercontent.com/hapin-trial/thresholds/master/thresholds.R")

#source thresholds locally
# source('thresholds.R')

#source the functions file from github
source_url("https://raw.githubusercontent.com/hapin-trial/thresholds//master/functions.R")

#source functions locally
# source('functions.R')