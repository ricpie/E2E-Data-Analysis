# HHID resolutions

predicted_ses<-readRDS("Processed Data/predicted_ses.rds")
preplacement <- as.data.table(readRDS("Processed Data/preplacement.rds"))
preplacement <- merge(predicted_ses,preplacement,by="HHID",all.y = TRUE)
preplacement <- preplacement[!duplicated(preplacement[5:30]),]
mobenzi_indepth <- readRDS("Processed Data/mobenzi_indepth.rds")
rapid <- mobenzi_indepth <- readRDS("Processed Data/mobenzi_rapid.rds")


#Read in rapid survey data sent by Matt.  Should be able to cross-reference with 

MattRapidFiles <-list.files('Processed Data/Mobenzi Data/hhidsrandomizedforindepthsurveyeldoret',full.names = T)
MattRapid = rbind(fread(MattRapidFiles[3],fill=TRUE),fread(MattRapidFiles[5],fill=TRUE),fread(MattRapidFiles[5],fill=TRUE))

MattRapid <- rbindlist(lapply(MattRapidFiles, fread))

# Remove the submissions I do not want matching
mobenzi_indepth <-mobenzi_indepth %>% dplyr::filter(Submission.Id != "f0c178cb-1f1d-4762-86e2-d53c01721db3", #Resolved KE001-KE07
                                                    Submission.Id != "347578db-ed13-4d57-87dc-fe96d4f3fa77",	#Resolved with phone number
                                                    Submission.Id != "dbdc338a-4a26-42ba-b5e0-1ab4e8e6788e",	#Resolved with GPS from rapid survey and phone number from rapid survey agrees with preplacement survey.  No preplacement GPS.
                                                    Submission.Id != "c66e4549-9fe8-4470-b9a4-05035c8be03b", #Resolved with phone number	
                                                    Submission.Id != "863e7ee6-c60d-4678-88d6-339ad318339c", #Resolved with phone number	
                                                    Submission.Id != "b05df693-8132-4b45-a286-5978ec726b20"	) #Resolved by seeing that the indepth and preplacement GPS and HHID agreed, while the rapid survey actually did not.  Need to figure out the rapid survey issue.

preplacementtest <- merge(preplacement[,c(1,2,4,14:15)],mobenzi_indepth[,c(1:3,363:365)],by="HHID",all.y = FALSE)
predupes <- preplacementtest[duplicated(preplacementtest$HHID) | duplicated(preplacementtest$HHID,fromLast = T),]


# KE036-KE01 - no GPS.  fd8e1daa-a681-4a44-bb1e-8eb06c4b4470	 863e7ee6-c60d-4678-88d6-339ad318339c	
# KE048-KE05 - no GPS/phone.  This is actually listed as KE48-KE05	in the rapid survey.  Must modify the data and rerun the SES
# KE053 - no problem.
# KE148 - no problem
# KE238-KE04 Resolved.
# KE507-KE03 Resolved




