#Beacon data QA/QC
#Jiawen Liao with HAPIN team
#Emory University
#May 2019

## READ ME FIRST##
# Before running this file, change your working directory to the folder containing this file
# You can do this by typing setwd("/path/to/directory") or 
# by clicking command-D on the MacOS and selecting the directory or
# by going to File -> Change dir... in th R GUI on windows or
# by typing Ctrl+Shift+h on on RStudio for Windows or MacOS
##

#2018 August: Update Beacon check criteria using new criteria and update QA function

#Based on HAPIN SOP EXP-13
##source packages needed
#change the directory to the folder containning the script
#setwd("C:/Users/jliao8/Google Drive/Github/Beacon_QAQC")
source('load.R')
##pre_load beacon inventory, need to be changed if more beacons are added to inventory
beacon_inv = fread("Beacon_emitter_id_230619.csv",colClasses = c('character','character'))


##function

H41_file = list.files("redcap_data/")
H41_readin = function(H41_file){
      if(grepl('.xlsx',H41_file)){
            H41 = as.data.table(read_excel(paste("redcap_data/",H41_file,sep = "")))
      }
      if(grepl('.csv',H41_file)){
            H41 = fread(paste("redcap_data/",H41_file,sep = ""))
      }
      
      H41_beacon = H41[,.(id,redcap_event_name,h41_m_beacon_id1,h41_m_beacon_id2,h41_c_beacon_id1,h41_c_beacon_id2)]
      H41_beacon[,h41_m_beacon_id1:= as.character(h41_m_beacon_id1)]; H41_beacon[,h41_m_beacon_id2:= as.character(h41_m_beacon_id2)]; H41_beacon[,h41_c_beacon_id1:= as.character(h41_c_beacon_id1)]; H41_beacon[,h41_c_beacon_id2:= as.character(h41_c_beacon_id2)]
      
      H41_beacon[H41_beacon=='888'] = NA
      return(H41_beacon)
       
}
H41_data = H41_readin(H41_file)


#a function to slice 5 digit of Beacon id, will add 0 in in front if less than n (n = 5) digits
id_slicer_add <- function(x, n){
      str2 = substr(x, nchar(x)-n+1, nchar(x))
      str2 = str_pad(str2,n,'left',pad = '0')
      return(str2)
}



#revise a little bit
H41_data[redcap_event_name == 'b1_arm_2',phase := '4']
#add more of the study phase from redcap H41
H41_data[,redcap_event_name := NULL]

#test
#Beacon_file = Beacon_list[2]
Beacon_logger2_emitter_all = NULL
ID_missing_beacons = NULL

Beacon_QA = function(Beacon_file, beacon_inventory = beacon_inv,tz= 'UTC'){
      message(Beacon_file)
      if(nchar(Beacon_file) > 35){message('File name not correct, check the file')
            return(NULL)}
      Beacon_logger = fread(paste('Beacon_logger_data/',Beacon_file,sep = ""),col.names = c('Time',"MAC","RSSI"),skip = 1)
      Beacon_logger[,MAC := toupper(MAC)]
      
      #meta data
      hhid =strsplit(Beacon_file,"_")[[1]][1]
      logger_location = strsplit(Beacon_file,"_")[[1]][2]
      phase_selected =  strsplit(Beacon_file,"_")[[1]][3]
      logger_id =  strsplit(Beacon_file,"_")[[1]][4]
      sample_date =  strsplit(strsplit(Beacon_file,"_")[[1]][5],'.csv')[[1]][1]
      
      #select corresponding h41
      H41_selected = H41_data[phase == as.numeric(phase_selected) & id == hhid ,]
      #transform the data
      #select last 5 digit, if not 5 digits, add 0 to it
      Beacon_id_v = c(id_slicer_add(H41_selected$h41_m_beacon_id1,5),
                      id_slicer_add(H41_selected$h41_m_beacon_id2,5),
                      id_slicer_add(H41_selected$h41_c_beacon_id1,5),
                      id_slicer_add(H41_selected$h41_c_beacon_id2,5))
      location_v = c('PEM1','PEM2','PEC1','PEC2')
      #new dataset containning Beacon information
      H41_selected2 = data.table(cbind.data.frame(location_v,Beacon_id_v))
      rm(Beacon_id_v);rm(location_v)
      #match with MAC address from Beacon inventory
      H41_selected2 = merge(H41_selected2,beacon_inventory,by.x='Beacon_id_v',by.y = 'beacon_id',all.x = T, all.y = F)
      
      #check whether timezone is GMT/UTC
      if(!grepl('GMT',Beacon_logger[1,Time])){datetime_flag = 1}else{datetime_flag = 0}
      #check Time series should be 20s, and should have at least have ~6 hours of non-zero data for the MAC address and RSSI values
      #only select beacon emitter in the inventory
      Beacon_logger2 = Beacon_logger[MAC %in% beacon_inventory$MAC,]
      if(nrow(Beacon_logger2)==0){
            message('No Beacon emitter found, please check Beacon inventory and Beacon logger file, Check ID_missing_beacons for more information')
            #store MAC address
            ID_missing_MAC = unique(Beacon_logger$MAC)
            #only select MAC start with 0E and 0C
            ID_missing_MAC = ID_missing_MAC[ substr(ID_missing_MAC,1,2) %in% c('0E','0C')] 
            ID_missing_MAC = as.data.table(ID_missing_MAC)
            colnames(ID_missing_MAC) = c('MAC')
            ID_missing_MAC[,hhid := hhid];ID_missing_MAC[,phase := phase_selected]
            ID_missing_beacons <<- rbindlist(list(ID_missing_beacons,ID_missing_MAC))
            
            #add other flag = 1
            completeness_fl = 1;time_interval_fl = 1;beacon_presence_fl=1;beacon_time=1;startup_proc_fl=1;duration_fl=1
      }else{
            Beacon_logger2[,Time := ymd_hms(Time)]
            #create a new data by each beacon emitter, specifying time interval of data logging, RSSI mean and logging time for each emitter
            Beacon_logger2_emitter = Beacon_logger2[,.(diff_time = as.numeric(names(sort(table(diff(Time)),decreasing = T)[1])), sample_length = .N,
                                                       RSSI_mean = mean(RSSI))
                                                    , by = MAC]
            #is na exist for diff_time, change to 0 to flag but not clog the code
            Beacon_logger2_emitter[is.na(diff_time),diff_time := 0]
            #add some meta data identifier
            Beacon_logger2_emitter[,hhid:=hhid ];Beacon_logger2_emitter[,phase:=phase_selected ]
            #merge with H41, only keep MAC occured in both of inputs
            Beacon_logger2_emitter = merge(Beacon_logger2_emitter,H41_selected2,by= 'MAC', all.x = F, all.y = F )
            
            Beacon_logger2_emitter[,sample_hour := (diff_time)/60/60*sample_length]
            Beacon_logger2_emitter[,sample_length:= NULL]
            #only select 4 beacon emitter, with 4th longgest logging time
            Beacon_logger2_emitter[,time_rank := frank(-sample_hour)]
            Beacon_logger2_emitter = Beacon_logger2_emitter[time_rank %in% c(1:4)]
            
            #completeness flag: should have 6 hours of data logged for this logger
            if(sum(Beacon_logger2_emitter$sample_hour)<6){completeness_fl =1}else{completeness_fl = 0}
            
            #time_interval flag: data should be logged in every 15 - 45 second (some are 20 some are 40)
            if((sum(Beacon_logger2_emitter$diff_time<15) + sum(Beacon_logger2_emitter$diff_time>45))>0){time_interval_fl =1}else{time_interval_fl = 0}
            
            #beacon_presence flag: if for this logger, beacon emitter is not logged
            if(nrow(Beacon_logger2_emitter)<4){beacon_presence_fl = 1}else{beacon_presence_fl= 0}
            #check whether at least 1 PEC and PEM beacon exist, or set beacon exist flag to 2
            if(('M' %in% unique(substr(Beacon_logger2_emitter$location_v,3,3))) ==F | 
               ('C' %in% unique(substr(Beacon_logger2_emitter$location_v,3,3))) ==F){beacon_presence_fl = 2}
            
            #beacon_time flag: Data from all Beacons deployed should be visible for at least 1 hour of the deployment, need to have 4 Beacon emitters
            if(min(Beacon_logger2_emitter$sample_hour)<1){beacon_time = 1}else{beacon_time = 0}
            
            #startup_procedure flag: no beacon has RSSI greater than -75, loggers are placed to further away or obstruction
            if(max(Beacon_logger2_emitter$RSSI_mean) < -75){startup_proc_fl = 1}else{startup_proc_fl=0}
            #duration flag: check sample duration
            sample_duration = as.numeric(difftime( Beacon_logger2[nrow(Beacon_logger2),Time],Beacon_logger2[1,Time],units = 'hour'))
            if(sample_duration>26 | sample_duration<22){duration_fl = 1}else{duration_fl=0}
            
            #create a data frame to store all Beacon_emitter information for diagnosis, not used for reporting
            Beacon_logger2_emitter_all <<- rbind(Beacon_logger2_emitter_all,Beacon_logger2_emitter)
            
      }
      
      temp_data = cbind(hhid,phase_selected,logger_location,logger_id,sample_date,datetime_flag,completeness_fl,time_interval_fl,beacon_presence_fl,beacon_time,startup_proc_fl,duration_fl)
      
      return(temp_data)    
}
Beacon_list = list.files("Beacon_logger_data/")

Beacon_QA = as.data.table(ldply(Beacon_list, Beacon_QA, .progress = 'text'))

#save
todays_date <- gsub("-", "", as.character(Sys.Date()))
write.csv(Beacon_QA, file=paste("beacon_qa", "_", todays_date, ".csv", sep=''))


