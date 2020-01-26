if (!require("pacman")) install.packages("pacman")
pacman::p_load(lubridate,plyr,dplyr,reshape2,devtools,shiny,shinydashboard,dygraphs,DT,shinyjs,tools,data.table,writexl,zoo,mailR,readxl
,gmailr,mailR,cronR,miniUI,shinyFiles,ggplot2,stringr,chron,doParallel,foreach,openxlsx,gridExtra,drake)

cl <- makeCluster(detectCores()[1])
registerDoParallel(cl) 

