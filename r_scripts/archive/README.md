# Shiny ECM QA-QC App

A Shiny app to import, clean, and run minimal QA-QC checks on ECM files deployed as part of the HAPIN trial. 

### Running the app

The app uses a few packages that need to be installed before first run:

```
library(lubridate); library(plyr); library(reshape2); library(devtools);
library(shiny); library(shinydashboard); library(dygraphs); library(DT);
library(shinyjs); library(tools); library(data.table); library(writexl); 
library(zoo)
```

Alternately, run the code below to download and install packages that are not currently installed.

```
check.packages <- function(pkg){
    new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
    if (length(new.pkg)) 
        install.packages(new.pkg, dependencies = TRUE)
}

packages  <-  c('lubridate', 'plyr', 'reshape2', 'devtools', 'shiny', 'shinydashboard', 'dygraphs', 'DT', 'shinyjs', 'tools', 'data.table', 'writexl','zoo')

check.packages(packages)

```

From R (with the above packages installed), you can launch a local instance of the app from an internet-connected computer:

```
setwd('\path\to\ecm_checker')
library(devtools)
library(shiny)
runGitHub('ecm_checker', 'ajaypillarisetti')
```
