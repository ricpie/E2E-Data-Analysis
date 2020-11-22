if (!require("pacman")) install.packages("pacman")
pacman::p_load(lubridate,plyr,dplyr,reshape2,devtools,shiny,shinydashboard,dygraphs,DT,shinyjs,tools,data.table,writexl,zoo,readxl
,gmailr,mailR,ggplot2,stringr,doParallel,foreach,openxlsx,gridExtra,egg,cowplot,ggbiplot,corrgram,
factoextra,scales,htmlwidgets,tidyfast,tidyr)

registerDoParallel(cores=detectCores()-1)

