
parse_filename_fun <- function(file){
  tryCatch({ 
    filename = data.table()
    filename$fullname = gsub(' ','',file) 
    filename$fullname = gsub('.txt','',filename$fullname)
    filename$fullname = gsub('.csv','',filename$fullname)
    filename$datestart = ymd(strsplit(basename(filename$fullname), "_")[[1]][1])
    filename$basename = basename(filename$fullname)
    filename$basename_sansext = file_path_sans_ext(filename$basename)
    filename$sampleID = strsplit(filename$basename_sansext , "_")[[1]][2]
    filename$loggerID = strsplit(filename$basename_sansext , "_")[[1]][3]
    if(is.na(filename$loggerID)){filename$loggerID = 'Missing'}
    filename$filterID = strsplit(filename$basename_sansext , "_")[[1]][4] 
    filename$sampletype= strsplit(basename(filename$sampleID), "-")[[1]][3]
    filename$fieldworkerIDstr = strsplit(basename(filename$sampleID), "-")[[1]][2]
    matches <- regmatches(filename$fieldworkerIDstr, gregexpr("[[:digit:]]+", filename$fieldworkerIDstr))
    filename$fieldworkerID = as.numeric(matches)
    filename$HHIDstr = strsplit(basename(filename$sampleID), "-")[[1]][1]
    matches <- regmatches(filename$HHIDstr, gregexpr("[[:digit:]]+", filename$HHIDstr))
    filename$HHID =  as.numeric(matches)
    filename$flag = strsplit(filename$basename_sansext , "_")[[1]][5]
    if(is.na(filename$flag)) {filename$flag = c("good")}
    num_underscores <- lengths(regmatches(filename$basename, gregexpr("_", filename$basename)))
    if ( num_underscores > 3 |  (is.na(filename$filterID) & num_underscores>3) | is.na(filename$HHID) | filename$fieldworkerID > 25 |
         filename$fieldworkerID < 0) {filename$filename_flag = 1}else{filename$filename_flag = 0}
    filename
    return(filename)
  }, error = function(e) {
    print('Error parsing filename')
    print(file)
    return(NULL)
  })  
}


equipment_IDs_fun <- function(){
  # Import equipment ID info
  
  #separate out raw, meta, and sensor data
  equipmentIDpath<- "~/Dropbox/UNOPS emissions exposure/Equipment/E2E_BA equipment matrix_vF.xlsx"
  # equipmentIDs <- equipment_IDs_fun(equipmentIDpath)  
  equipment_IDs <- read_excel(equipmentIDpath, sheet = "Equipment IDs")[,c(2:4)]
  setnames(equipment_IDs,c("loggerID","BAID","instrument"))
  saveRDS(equipment_IDs,"Processed Data/equipment_IDs.rds")
  equipment_IDs
}


mobenzi_import_fun <- function(output=c('mobenzi_indepth', 'mobenzi_rapid','preplacement','postplacement')){
  #Paths
  fileindepth <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Mobenzi Data/In Depth/Responses.csv"
  filerapid <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Mobenzi Data/Rapid/Responses.csv"
  preplacementpath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Mobenzi Data/Pre Placement/Responses.csv"
  postplacementpath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/Mobenzi Data/Post Placement/Responses.csv"
  
  preplacement <- read.table(preplacementpath, header=T,quote = "\"",sep=",",na.string=c("","null","NaN"),colClasses = "character")[,1:89]
  
  #Import and clean up preplacement
  setnames(preplacement,c("Submission_Id","Fieldworker_Name","Fieldworker_Id","Handset_Asset_Code","Handset_Identifier","Received","Start","End","Duration__seconds_","Latitude","Longitude","Language","Survey_Version","Modified_By","Modified_On","Complete","Worker","UNOPS_Date","UNOPS_HH","Something","Age","Gender","Childrenunder5YN","HealthConditions","OKtoWearHealthConditions","AwayFromHomeMoreThan5Hours","WillingToWearAwayFromHome","MicropemID","LascarID","UNOPSworkerPresent","OKLivingRoom","OkEmissions","EmissionsTime","UNOPSWorker","BeaconID1","BeaconID2","WalkThrough1Start","WalkThrough1End","WalkThrough2Start","WalkThrough2End","WalkThrough3Start","WalkThrough3End","HHID","WalkTimetoMainRoad","NoManufacturedStove","HasChimney","TraditionalStoveFAfrica/Nairobiures","FanStoveFAfrica/Nairobiures","KitchenLocation","MicropemIDKitchen","LascarIDKitchen","UNOPSyn","BeaconLoggerIDKitchen","IntensiveEquipmentYN","IntensiveYN","PATSorECMYN","ECMIDKitchen","PATSIDKitchen","ElectricYN","KeroseneYN","LPGYN","TraditionalYN","ManufacturedYN","TSFYN","OtherYN","OtherStoveType","MicropemDistanceCM","MicropemHeightCM","PictureYN","Number_Of_Children","ChildAge","ChildGender","Child2Age","Child2Gender","Child3Age","Child3Gender","ECMIDChild1","ECMIDChild2","ECMIDChild3","unknown","LivingRoomMonitoringYN","RoomTypeSecondary","WallTypeSecondary","PATSIDSecondary","LascarIDSecondary","BeaconLoggerIDSecondary","PATSDistanceFloorSecondaryCM","PicsSecondaryYN","DevicesONTime"))
  preplacement <- dplyr::filter(preplacement,!is.na(DevicesONTime)) %>%
    dplyr::mutate(HHID = gsub('000','00',HHID),
                  HHID = gsub(' ','',HHID),
                  HHID = gsub('O','0',HHID,ignore.case = T),
                  HHID = gsub('-0','-KE0',HHID),
                  HHID = gsub('-KE00','-KE0',HHID),
                  HHID = gsub('KR','KE',HHID),
                  HHID = gsub('-KE012','-KE12',HHID),
                  HHID = gsub('-KE010','-KE10',HHID),
                  HHID = gsub('-KE011','-KE11',HHID),
                  HHID = gsub('-KE013','-KE13',HHID),
                  HHIDstr = substring(HHID, 
                                      1, sapply(HHID, function(x) unlist(gregexpr('-',x,perl=TRUE))[1])-1),
                  WorkerIDstr = substring(HHID, 
                                          sapply(HHID, function(x) unlist(gregexpr('-',x,perl=TRUE))[1])+1,15),
                  matches = regmatches(HHIDstr, gregexpr("[[:digit:]]+", HHIDstr)),
                  HHIDnumeric =  as.numeric(matches),
                  start_datetime = as.POSIXct(paste(UNOPS_Date,DevicesONTime,sep = " "),tz = "Africa/Nairobi",
                                              tryFormats = c("%d-%m-%Y %H:%M","%d-%m-%Y %H:%M:%OS","%d-%m-%y %H:%M","%d/%m/%Y %H:%M:%OS"))) %>%
    dplyr::filter("Yes" == OkEmissions) %>% #,  !is.na(UNOPS_HH))
    dplyr::mutate(WalkThrough1Start = as.POSIXct(paste0(UNOPS_Date,' ',WalkThrough1Start),tz = "Africa/Nairobi",tryFormats = c("%d-%m-%Y %H:%M")),
                  WalkThrough1End = as.POSIXct(paste0(UNOPS_Date,' ',WalkThrough1End),tz = "Africa/Nairobi",tryFormats = c("%d-%m-%Y %H:%M")),
                  WalkThrough2Start = as.POSIXct(paste0(UNOPS_Date,' ',WalkThrough2Start),tz = "Africa/Nairobi",tryFormats = c("%d-%m-%Y %H:%M")),
                  WalkThrough2End = as.POSIXct(paste0(UNOPS_Date,' ',WalkThrough2End),tz = "Africa/Nairobi",tryFormats = c("%d-%m-%Y %H:%M")),
                  WalkThrough3Start = as.POSIXct(paste0(UNOPS_Date,' ',WalkThrough3Start),tz = "Africa/Nairobi",tryFormats = c("%d-%m-%Y %H:%M")),
                  WalkThrough3End = as.POSIXct(paste0(UNOPS_Date,' ',WalkThrough3End),tz = "Africa/Nairobi",tryFormats = c("%d-%m-%Y %H:%M")))
  
  
  #Clean up postplacement HHIDs
  postplacement <- read.table(postplacementpath, header=T,quote = "\"",sep=",",na.string=c("","null","NaN"),colClasses = "character") %>%
    dplyr::mutate( HHID = gsub('000','00',Household.Study.ID),
                   HHID = gsub(' ','',HHID),
                   HHID = gsub('O','0',HHID,ignore.case = T),
                   HHID = gsub('-0','-KE0',HHID),
                   HHID = gsub('-KE00','-KE0',HHID),
                   HHID = gsub('KR','KE',HHID),
                   HHID = gsub('-KE012','-KE12',HHID),
                   HHID = gsub('-KE010','-KE10',HHID),
                   HHID = gsub('-KE011','-KE11',HHID),
                   HHID = gsub('-KE013','-KE13',HHID),
                   HHID = gsub('025-KE05','KE025-KE05',HHID),
                   HHID = gsub('KEKE','KE',HHID)) 
  
  
  #Clean up rapid survey HHIDs
  mobenzi_rapid <- read.table(filerapid, header=T,quote = "\"",sep=",",na.string=c("","null","NaN"),colClasses = "character") %>%
    setnames(c('SubmissionId','FieldworkerName','FieldworkerId','HandsetAssetCode','HandsetIdentifier','Received','Start','End','Duration(seconds)','Latitude','Longitude','Language','SurveyVersion','ModifiedBy','ModifiedOn','Complete','Accept','HHID','SubCounty','Village','Date','StaffCode','Gender','Age','MaritalStatus','Eth','Other23','HeadofHousehold',
               'AgeHeadofHousehold','EducationHeadofHousehold','Other19','RespondentEducation','Other1','University Student','Relocation','Holidays','MainCookYN','DecisionMakerFuel','Other2','PeopleinHousehold','PeopleEatinginHome','ChildrenUnder5','ChildAges','OwnorRent','Other3','HeadofHouseholdIncome','C3_Farmsownland(whetherthatlandisownedorrented)','C3_Daylabourer(farminganotherperson’sland,builder,dailyworkeretc.)',
               'C3_Governmentemployee(doctor,nurse,police,teacheretc.)','C3_Employeeinabusiness(Factory/industrialworker,worksinashop,receptionist,securityguard,etc.)','C3_Hasownbusiness(businessman,ownsashop,traderetc.)','C3_Craftsperson(tailor,carpenter,seamstressetc.)','C3_Runthehousehold/Careforfamily','C3_Retired','C3_Othertypeofjob','C3_Currentlyunemployed/nothing','Other4',
               'Income','SeasonalIncomeYN','C6_Animal(s)(cows,sheep,goatsetc.)','C6_Cellphone','C6_Smartphone','C6_Radio','C6_Hi-Fi/CD-player','C6_Solarconnection','C6_ElectricityConnection','C6_TV','C6_SatelliteTV','C6_Refrigerator/fridge/freezer','C6_Shower/bathwithinhouse','C6_Land','C6_Bicycle','C6_Moped/Motorcycle','C6_Pick-uptruck','C6_Car','C6_Computer','C6_Washingmachine',
               'C6_Tractor','WaterAccessYN','WaterSource','Other5','SepticorFlushingToiletInside','LatrineinCompound','FoodConsumedMadeinHousehold','SourceofOutsideFood','MostUsedCookstove','Other20','MostUsedFuel','Other6','TimesFuelUnavailableLastYEar','D5_1.None','D5_2Adultburned','D5_3Childburned','D5_4Adultscalded','D5_5Childscalded','D5_6Fireinhouse',
               'D5_7Poisoning','D5_8Death','D5_9Other,specify','Other18','D6_Electricity','D6_Kerosene','D6_Cookinggas/LPG','D6_Charcoalunprocessed','D6_Charcoalbriquettes/pellets','D6_Wood','D6_Agriculturalorcropresidue/grass/','D6_straw/shrubs/corncobs','D6_Processedbiomasspellets/briquettes','D6_Woodchips','D6_Sawdust','D6_Animalwaste/dung','D6_Garbage/plastic','D6_None',
               'D6_Otherspecify','Other7','IsFuelFree','D8_Electricity','D8_Kerosene','D8_Cookinggas/LPG','D8_Charcoalunprocessed','D8_Charcoalbriquettes/pellets','D8_Wood','D8_Agriculturalorcropresidue/grass/','D8_straw/shrubs/corncobs','D8_Processedbiomasspellets/briquettes','D8_Woodchips','D8_Sawdust','D8_Animalwaste/dung','D8_Garbage/plastic','D8_Other,specify','DoYouPayForFuel',
               'D12_Electricity','D12_Kerosene','D12_Cookinggas/LPG','D12_Charcoalunprocessed','D12_Charcoalbriquettes/pellets','D12_Wood','D12_Agriculturalorcropresidue/grass/','D12_straw/shrubs/corncobs','D12_Processedbiomasspellets/briquettes','D12_Woodchips','D12_Sawdust','D12_Animalwaste/dung','D12_Garbage/plastic','D12_Other,specify','Other16','WhereCook','Other8','SharedKitchenYN',
               'HeatingDeviceEver','HeatingUseMonths','HeaterType','Other17','LightingSource','Other9','UseLPG','NumberCylindersAtHome','F3_3kg','F3_6kg','F3_13kg','F3_16kg','F3_Other,specify','Other10','LPGRefillCost','LPGRefillInterval','Other11','LPGRefillsPerYear','LPGWhenPurchased','LPGBurners','LPGStoveLastPurchase','LPGDaysUsedLastWeek','LPGHowTransported','Other12','LPGTransportDuration'
               ,'LPGTravelCost','LPGTravelCostKSH','LPGDeliverCost','LPGHaveYouUsedIt','LPGWhyNotUsing','Other13','LPGWhoWouldDecideToUse','Other14','WhyNoInterview','Other15','CanWeContactYouAgain','Phone','GPS','Latitude2','Longitude2','AltitudeMeters','DateofGPS','Notes','FloorMaterial','Other21','RoofingMaterial','Other22','WallMaterial','Other24')) %>%
    dplyr::filter(Complete == "Yes",
                  Accept == "Yes") %>% #,
    # FoodConsumedMadeinHousehold == "Yes") %>%
    dplyr::mutate(HHID = toupper(HHID)) %>%
    dplyr::mutate(HHID = gsub('KE0004-KE01','KE001-KE10',HHID),
                  HHID = gsub('KE05-KE02','KE001-KE08',HHID),
                  HHID = gsub('000','00',HHID),
                  HHID = gsub(' ','',HHID),
                  HHID = gsub('_','-',HHID),
                  HHID = gsub('O','0',HHID,ignore.case = T),
                  HHID = gsub('KES','KE',HHID,ignore.case = T),
                  HHID = gsub('-0','-KE0',HHID),
                  HHID = gsub('-KE00','-KE0',HHID),
                  HHID = gsub('KR','KE',HHID),
                  HHID = gsub('-KE012','-KE12',HHID),
                  HHID = gsub('-KE010','-KE10',HHID),
                  HHID = gsub('-KE011','-KE11',HHID),
                  HHID = gsub('-KE013','-KE13',HHID),
                  HHID = gsub('025-KE05','KE025-KE05',HHID),
                  HHID = gsub('KE-KE','KE',HHID),
                  HHID = gsub('KEKE','KE',HHID),
                  HHID = gsub("'Ķ",'KE157-KE02	',HHID),
                  HHID = gsub('LE','KE',HHID),
                  HHID = gsub('KEKE','KE',HHID),
                  HHID = case_when(
                    HHID == '1' ~ 'KE001-KE03',
                    HHID == '2' ~ 'KE002-KE03',
                    HHID == '3' ~ 'KE003-KE03',
                    HHID == '4' ~ 'KE004-KE03',
                    HHID == '5' ~ 'KE005-KE03',
                    HHID == '6' ~ 'KE006-KE03',
                    HHID == '7' ~ 'KE007-KE03',
                    HHID == '8' ~ 'KE008-KE03',
                    HHID == '9' ~ 'KE009-KE03',
                    HHID == '10' ~ 'KE010-KE03',
                    HHID == '001' ~ 'KE001-KE03',
                    HHID == '002' ~ 'KE002-KE03',
                    HHID == '003' ~ 'KE003-KE03',
                    HHID == '004' ~ 'KE004-KE03',
                    HHID == '005' ~ 'KE005-KE03',
                    HHID == '006' ~ 'KE006-KE03',
                    HHID == '007' ~ 'KE007-KE03',
                    HHID == '008' ~ 'KE008-KE03',
                    HHID == '009' ~ 'KE009-KE03',
                    HHID == '010' ~ 'KE010-KE03',
                    HHID == 'KE026' ~ 'KE026-KE03',
                    HHID == 'KE023' ~ 'KE023-KE04',
                    HHID == 'KE033' ~ 'KE033-KE04',
                    HHID == 'KE041' ~ 'KE041-KE04',
                    HHID == 'KE042' ~ 'KE042-KE04',
                    HHID == 'KE038' ~ 'KE038-KE02',
                    HHID == 'KE039' ~ 'KE039-KE04',
                    HHID == 'KE056' ~ 'KE056-KE04',
                    HHID == 'KE057' ~ 'KE057-KE04',
                    HHID == 'KE058' ~ 'KE057-KE04',
                    HHID == 'KE061' ~ 'KE061-KE04',
                    HHID == 'KE062' ~ 'KE062-KE04',
                    HHID == 'KE110' ~ 'KE110-KE04',
                    HHID == 'KE209KE04' ~ 'KE209-KE04',
                    HHID == 'KE039' ~ 'KE039-KE04',
                    HHID == 'KE01-KE024' ~ 'KE024-KE01', 	
                    HHID == 'KE01-KE066' ~ 'KE066-KE01', 	
                    HHID == 'KE01-KE068' ~ 'KE068-KE01', 	
                    HHID == 'KE01-KE069' ~ 'KE069-KE01', 	
                    HHID == 'KE01-KE076' ~ 'KE076-KE01', 	
                    HHID == 'KE01-KE085' ~ 'KE085-KE01', 	
                    HHID == 'KE01-KE086' ~ 'KE086-KE01', 	
                    HHID == 'KE01-KE087' ~ 'KE087-KE01', 	
                    HHID == 'KE01-KE088' ~ 'KE088-KE01', 	
                    HHID == 'KE01-KE089' ~ 'KE089-KE01', 	
                    HHID == 'KE01-KE090' ~ 'KE090-KE01', 	
                    HHID == 'KE01-KE091' ~ 'KE091-KE01', 	
                    HHID == 'KE01-KE092' ~ 'KE092-KE01', 	
                    HHID == 'KE01-KE093' ~ 'KE093-KE01', 	
                    HHID == 'KE01-KE094' ~ 'KE094-KE01', 	
                    HHID == 'KE01-KE095' ~ 'KE095-KE01', 	
                    HHID == 'KE01-KE096' ~ 'KE096-KE01', 	
                    HHID == 'KE01-KE097' ~ 'KE097-KE01', 	
                    HHID == 'KE01-KE098' ~ 'KE098-KE01', 	
                    HHID == 'KE01-KE099' ~ 'KE099-KE01', 	
                    HHID == 'KE01-KE100' ~ 'KE100-KE01', 	
                    HHID == 'KE01-KE101' ~ 'KE101-KE01', 	
                    HHID == 'KE01-KE102' ~ 'KE102-KE01', 	
                    HHID == 'KE01-KE103' ~ 'KE103-KE01', 	
                    HHID == 'KE01-KE104' ~ 'KE104-KE01', 	
                    HHID == 'KE01-KE105' ~ 'KE105-KE01', 	
                    HHID == 'KE01-KE106' ~ 'KE106-KE01', 	
                    HHID == 'KE01-KE107' ~ 'KE107-KE01', 	
                    HHID == 'KE01-KE108' ~ 'KE108-KE01', 	
                    HHID == 'KE01-KE109' ~ 'KE109-KE01', 	
                    HHID == 'KE01-KE110' ~ 'KE110-KE01', 	
                    HHID == 'KE01-KE111' ~ 'KE111-KE01', 	
                    HHID == 'KE01-KE112' ~ 'KE112-KE01', 	
                    HHID == 'KE01-KE113' ~ 'KE113-KE01', 	
                    HHID == 'KE01-KE114' ~ 'KE114-KE01', 	
                    HHID == 'KE01-KE115' ~ 'KE115-KE01', 	
                    HHID == 'KE01-KE116' ~ 'KE116-KE01', 	
                    HHID == 'KE01-KE117' ~ 'KE117-KE01', 	
                    HHID == 'KE01-KE118' ~ 'KE118-KE01', 	
                    HHID == 'KE01-KE119' ~ 'KE119-KE01', 	
                    HHID == 'KE01-KE120' ~ 'KE120-KE01', 	
                    HHID == 'KE01-KE121' ~ 'KE121-KE01', 	
                    HHID == 'KE01-KE122' ~ 'KE122-KE01', 	
                    HHID == 'KE01-KE123' ~ 'KE123-KE01', 	
                    HHID == 'KE01-KE124' ~ 'KE124-KE01', 	
                    HHID == 'KE01-KE125' ~ 'KE125-KE01', 	
                    HHID == 'KE01-KE126' ~ 'KE126-KE01', 	
                    HHID == 'KE01-KE127' ~ 'KE127-KE01', 	
                    HHID == 'KE01-KE128' ~ 'KE128-KE01', 	
                    HHID == 'KE01-KE129' ~ 'KE129-KE01', 	
                    HHID == 'KE01-KE130' ~ 'KE130-KE01', 	
                    HHID == 'KE01-KE131' ~ 'KE131-KE01', 	
                    HHID == 'KE01-KE132' ~ 'KE132-KE01', 	
                    HHID == 'KE01-KE133' ~ 'KE133-KE01', 	
                    HHID == 'KE01-KE134' ~ 'KE134-KE01', 	
                    HHID == 'KE01-KE135' ~ 'KE135-KE01',
                    HHID == 'KE01-KE136' ~ 'KE136-KE01',
                    HHID == 'KE03-KE198' ~ 'KE198-KE03',
                    HHID == 'KE05-KE014' ~ 'KE014-KE05',
                    HHID == 'KE09-KE029' ~ 'KE029-KE09',
                    HHID == 'KE05-KE08' ~ 'KE008-KE05',
                    HHID == 'KE05-KE06' ~ 'KE006-KE05',
                    HHID == 'KE05-KE13' ~ 'KE013-KE05',
                    HHID == 'KE025' ~ 'KE025-KE04',
                    HHID == 'KE05-KE016' ~ 'KE016-KE05',
                    HHID == 'KE05-KE017' ~ 'KE017-KE05',
                    HHID == 'KE05-KE018' ~ 'KE018-KE05',
                    HHID == 'KE05-KE019' ~ 'KE019-KE05',
                    HHID == 'KE05-KE02' ~ 'KE020-KE05',
                    HHID == 'KE28-KE05'	~ 'KE028-KE05',
                    HHID == 'KE05-KE021' ~ 'KE021-KE05',
                    HHID == 'KE225' ~ 'KE225-KE08',
                    HHID == '43' ~ 'KE043-KE07',
                    HHID == 'KE026' ~ 'KE026-KE03',
                    HHID == 'KE210-KE04' ~ 'KE210-KE02',
                    HHID == 'KE003-KE01' ~ 'KE001-KE07',
                    HHID == 'KE152-KE04'	~ 'KE154-KE04',
                    HHID == 'KE507-KE09' ~ 'KE507-KE03',
                    TRUE ~ as.character(HHID)
                  )
    )  %>%
    dplyr::select(-Language,-SurveyVersion,-FieldworkerId,-Accept,-Complete) %>%
    # dplyr::mutate(matches = regmatches(HHIDstr, gregexpr("[[:digit:]]+", HHIDstr)),
    #               HHIDnumeric =  as.numeric(matches)) %>%
    dplyr::arrange(Start) %>%
    dplyr::mutate(HHIDstr = substring(HHID, 
                                      1, sapply(HHID, function(x) unlist(gregexpr('-',x,perl=TRUE))[1])-1),
                  WorkerIDstr = substring(HHID, 
                                          sapply(HHID, function(x) unlist(gregexpr('-',x,perl=TRUE))[1])+1,15),
                  WorkerID_StaffCode = substring(StaffCode,1,4)) %>%
    dplyr::mutate(EducationHeadofHousehold = as.numeric(factor(EducationHeadofHousehold,c("No formal education","Other, specify","Primary","Secondary/High school",
                                                                                          "University (Under graduate, Post graduate)")))) %>%
    dplyr::mutate(RespondentEducation = as.numeric(factor(RespondentEducation,c("No formal education","Other, specify","Primary","Secondary/High school",
                                                                                "University (Under graduate, Post graduate)")))) %>%
    dplyr::mutate(EducationHighest = 
                    case_when(
                      is.na(EducationHeadofHousehold) ~ RespondentEducation,
                      EducationHeadofHousehold >= RespondentEducation ~ EducationHeadofHousehold,
                      is.na(RespondentEducation) ~ 1,
                      TRUE ~ EducationHeadofHousehold
                    )) %>%
    dplyr::mutate(EducationHighest = as.factor(EducationHighest),
                  EducationHighest  = mapvalues(EducationHighest,from = c("1","2","3","4","5"), 
                                                to =c("No formal education","Other, specify","Primary","Secondary/High school",
                                                      "University (Under graduate, Post graduate)"))) %>%
    dplyr::mutate(OwnorRent = gsub('Other arrangement \\(please specify\\)','Rent',OwnorRent),
                  FloorMaterial = paste0('floor_',FloorMaterial),
                  RoofingMaterial = paste0('roofing_',RoofingMaterial),
                  WallMaterial = paste0('wall_',WallMaterial)) %>%
    dplyr::mutate(namelength = str_length(WorkerIDstr)) %>%
    dplyr::mutate(HHID = case_when(namelength>4 ~ paste0(WorkerIDstr,'-',HHIDstr),
                                   TRUE ~ as.character(HHID))) %>%
    dplyr::mutate(HHID = case_when(HHID == 'KE02	-KE157' ~ 'KE157-KE02',
                                   HHID == 'KE069KE09-' ~ 'KE069-KE09',
                                   HHID == 'KE210-' ~ 'KE210-KE08',
                                   HHID == 'KE018-' ~ 'KE018-KE06',
                                   HHID == '0' ~ 'KE068-KE10',
                                   HHID == '-KE10-KE068' ~ 'KE068-KE10',
                                   HHID == 'KE0103-KE06' ~ 'KE103-KE06',
                                   HHID == 'KE019-' ~ 'KE019-KE06',
                                   HHID == 'KE2019-KE07' ~ 'KE219-KE07',
                                   TRUE ~ as.character(HHID))) %>%
    dplyr::mutate(HHID = case_when(SubmissionId == 'ef03a2b1-c343-4d31-a08b-9266cd6545a0' ~ 'KE502-KE01',
                                   SubmissionId == 'a772a091-f97a-409d-ae08-6eadb8c12c6e' ~ 'KE507-KE06',
                                   SubmissionId == '996b457f-3b0f-4ccc-bbc3-a6df7d67f067' ~ 'KE510-KE09',
                                   TRUE ~ as.character(HHID)))
  
  
  #This data set is very messy - many cases of duplicated HHIDs, for different households. Retain all for analyses.
  # uniqueHHIDs <- mobenzi_rapid[duplicated(mobenzi_rapid$HHID) | duplicated(mobenzi_rapid$HHID,fromLast=TRUE),] %>%
  # dplyr::arrange(HHID)
  
  # badHHIDs <- dplyr::mutate(mobenzi_rapid,namelength = str_length(WorkerIDstr)) %>%
  #   dplyr::filter(namelength>4 | str_length(HHID)< 8)
  # goodHHIDs <- dplyr::mutate(mobenzi_rapid,namelength = str_length(WorkerIDstr)) %>%
  #   dplyr::filter(namelength<=4)
  # write.xlsx(badHHIDs,'badfileHHIDs.xlsx')
  
  
  #Import and clean up indepth survey HHIDs
  mobenzi_indepth <- read.table(fileindepth, header=T,quote = "\"" ,sep=",",na.string=c("","null","NaN"),colClasses = "character")%>%
    dplyr::mutate( HHID = gsub('000','00',Household.Study.ID),
                   HHID = gsub(' ','',HHID),
                   HHID = gsub('O','0',HHID,ignore.case = T),
                   HHID = gsub('-0','-KE0',HHID),
                   HHID = gsub('-KE00','-KE0',HHID),
                   HHID = gsub('KR','KE',HHID),
                   HHID = gsub('-KE012','-KE12',HHID),
                   HHID = gsub('-KE010','-KE10',HHID),
                   HHID = gsub('-KE011','-KE11',HHID),
                   HHID = gsub('-KE013','-KE13',HHID),
                   HHID = gsub('KE01-KE118','KE118-KE01',HHID),
                   HHID = gsub('KEKE','KE',HHID)) 
  
  saveRDS(mobenzi_indepth,"Processed Data/mobenzi_indepth.rds")
  saveRDS(mobenzi_rapid,"Processed Data/mobenzi_rapid.rds")
  saveRDS(preplacement,"Processed Data/preplacement.rds")
  saveRDS(postplacement,"Processed Data/postplacement.rds")
  write.xlsx(mobenzi_rapid,'Processed Data/mobenzi_rapid.xlsx')
  
  return(list(mobenzi_indepth=mobenzi_indepth, mobenzi_rapid=mobenzi_rapid,preplacement=preplacement,postplacement=postplacement))
}


# parse_mobenzi_fun <- function(preplacement,output = preplacement_meta){
#   preplacement_meta = list()
#   preplacement_meta$datestart = as.POSIXlt(preplacement$Start,tz="Africa/Nairobi","%d-%m-%Y")
#   preplacement_meta$beacon1ID = as.character(preplacement$)
#   preplacement_meta$beacon2ID = strsplit(basename(preplacement$basename), "_")[[1]][4] 
#   preplacement_meta$sampletype= strsplit(basename(preplacement$sampleID), "-")[[1]][3]
#   preplacement_meta$fieldworkerIDstr = strsplit(basename(preplacement_meta$sampleID), "-")[[1]][2]
#   matches <- regmatches(preplacement_meta$fieldworkerIDstr, gregexpr("[[:digit:]]+", preplacement_meta$fieldworkerIDstr))
#   preplacement_meta$fieldworkerID = as.numeric(matches)
#   preplacement_meta$HHIDstr = strsplit(basename(preplacement_meta$sampleID), "-")[[1]][1]
#   matches <- regmatches(preplacement_meta$HHIDstr, gregexpr("[[:digit:]]+", preplacement_meta$HHIDstr))
#   preplacement_meta$HHID =  as.numeric(matches)
#   preplacement_meta$sampleID = strsplit(basename(preplacement_meta$basename), "_")[[1]][2]
#   filename
# }


lascar_cali_import <- function(){
  lascarcalipath1 <- "~/Dropbox/UNOPS emissions exposure/Data/Calibrations/Lascar CO Calibration Template_8Aug_b_RP_trimmed.xlsx"
  lascarcalipath2 <- "~/Dropbox/UNOPS emissions exposure/Data/Calibrations/Lascar CO Calibration Template_8Aug_a_RP_trimmed.xlsx"
  lascar_cali_1 <- read_excel(lascarcalipath1,sheet = "CO cal Data",skip=4)[,c(2:5)]
  completerecords_1 <- na.omit(lascar_cali_1)
  lascar_cali_2 <- read_excel(lascarcalipath2,sheet = "CO cal Data",skip=4)[,c(2:5)]
  completerecords_2 <- na.omit(lascar_cali_2)
  
  lascar_cali_coefs = merge(completerecords_1,completerecords_2,all=TRUE) %>%
    dplyr::mutate(instrument = "Lascar") 
  setnames(lascar_cali_coefs,c("loggerID","COslope","R2","COzero","instument"))
  lascar_cali_coefs <- dplyr::mutate(lascar_cali_coefs,loggerID = paste0('LAS',loggerID)) %>%
    dplyr::mutate(loggerID = gsub("LASCAA","CAA",loggerID))
  
  saveRDS(lascar_cali_coefs,"Processed Data/lascar_calibration_coefs.rds")
  lascar_cali_coefs
}


apply_lascar_calibration<- function(file,loggerIDval,raw_data) {
  lascar_cali_coefs <- readRDS("Processed Data/lascar_calibration_coefs.rds")
  lascar_cali_coefs <- as.data.table(lascar_cali_coefs)
  logger_cali <- lascar_cali_coefs[loggerID=="loggerIDval",]
  if (!nrow(logger_cali)){
    logger_cali <- data.table(
      loggerID = NA,
      COslope = 1,
      COzero = 0,
      R2 = NA,
      instrument = NA)
  }
  calibrated_data <- as.data.table(dplyr::mutate(raw_data,CO_ppm = CO_raw*logger_cali$COslope + logger_cali$COzero)) %>%
    dplyr::select(-SampleNum,-CO_raw)
  calibrated_data
}

ambient_import_fun <- function(path_other,sheetname){
  meta_ambient <- read_excel(path_other,sheet = sheetname,skip=1)[,c(1:12)]
  setnames(meta_ambient,c('fieldworker_name','fieldworkerID','patsID','upasID','upas_filterID','lascarID','date_start','time_start','date_end','time_end','problems','comments'))
  meta_ambient <- meta_ambient[!is.na(meta_ambient$time_start),]
  meta_ambient$time_start <- strftime(meta_ambient$time_start, format="%H:%M:%S")
  meta_ambient$datetime_start <- as.POSIXct(paste(meta_ambient$date_start,meta_ambient$time_start,sep = " "),tz = "Africa/Nairobi")
  meta_ambient$time_end <- strftime(meta_ambient$time_end, format="%H:%M:%S",tz="Africa/Nairobi")
  meta_ambient$datetime_end <- as.POSIXct(paste(meta_ambient$date_end,meta_ambient$time_end,sep = " "),tz = "Africa/Nairobi")
  meta_ambient
}

emissions_import_fun <- function(path_emissions,sheetname,local_tz){
  meta_emissions <- read_excel(path_emissions,sheet = sheetname,skip=2)#[,c(1:12)]
  meta_emissions <- meta_emissions[!is.na(meta_emissions$`Date [m/d/y]`) & !is.na(meta_emissions$HH_ID)
                                   & !is.na(meta_emissions$`Sample START [hh:mm:ss]`),]
  meta_emissions$Date <- as.POSIXct(meta_emissions$`Date [m/d/y]`,tz=local_tz,tryFormats = c("%Y-%m-%d","%d-%m-%Y"))
  BG_initial_start_time <- strftime(meta_emissions$`Initial BG START [hh:mm:ss]`, format="%H:%M:%S",tz="UTC") #This is what it's coming in as.
  meta_emissions$datetime_BGi_start <- as.POSIXct(paste(meta_emissions$Date,BG_initial_start_time,sep = " "),tz = local_tz) #This is what we want it as.
  Sample_start_time <- strftime(meta_emissions$`Sample START [hh:mm:ss]`, format="%H:%M:%S",tz="UTC")
  meta_emissions$datetime_sample_start <- as.POSIXct(paste(meta_emissions$Date,Sample_start_time,sep = " "),tz = local_tz)
  Sample_end_time <- strftime(meta_emissions$`Sample END time`, format="%H:%M:%S",tz="UTC")
  meta_emissions$datetime_sample_end <- as.POSIXct(paste(meta_emissions$Date,Sample_end_time,sep = " "),tz = local_tz)
  BG_end_start_time <- strftime(meta_emissions$`Final Background start time`, format="%H:%M:%S",tz="UTC")
  meta_emissions$datetime_BGf_start <- as.POSIXct(paste(meta_emissions$Date,BG_end_start_time,sep = " "),tz = local_tz)
  datetimedecaystart <- strftime(as.character(meta_emissions$datetimedecaystart), format="%H:%M:%S",tz="UTC")
  meta_emissions$datetimedecaystart<-as.POSIXct(paste(meta_emissions$Date,datetimedecaystart,sep = " "),tz = local_tz)
  datetimedecayend <- strftime(as.character(meta_emissions$datetimedecayend), format="%H:%M:%S",tz="UTC")
  meta_emissions$datetimedecayend <- as.POSIXct(paste(meta_emissions$Date,datetimedecayend,sep = " "),tz = local_tz)
  meta_emissions$roomvolume <- meta_emissions$`Room length (longest) (m)`*meta_emissions$`Room height (m)`*meta_emissions$`Room width (shortest) (m)`
  meta_emissions$`# walls with open eaves`[is.na(meta_emissions$`# walls with open eaves`)] = 0
  meta_emissions$`# walls with open eaves` <- as.factor(meta_emissions$`# walls with open eaves`)
  meta_emissions$stovetype <- meta_emissions$`Stove Type...14`
  meta_emissions$stovetype <- gsub('TSF','Trad Biomass',meta_emissions$stovetype)
  meta_emissions$stovetype <- gsub('Chipkube','Trad Biomass',meta_emissions$stovetype)
  meta_emissions$eventduration <- difftime(meta_emissions$datetime_sample_end,meta_emissions$datetime_sample_start,units='mins')
  
  matches <- regmatches(meta_emissions$HHID_full, gregexpr("[[:digit:]]+", meta_emissions$HH_ID))
  meta_emissions$HHID =  as.numeric(matches)
  meta_emissions
}


grav_import_fun <- function(gravimetric_path){
  
  gravimetric_data <- read_excel(gravimetric_path,sheet = 'GRAVI',skip=0)
  setnames(gravimetric_data,c("filterID","BCµg2","BCµg3","BCµg4","BCµg5","Pre 1 (ug)","Pre 2 (ug)","Pre 3 (ug)","Range (max-min)...9","Average (ug)...10",
                              "...11","...12", "Post 1 (ug)",  "Post 2 (ug)",  "Post 3 (ug)", 
                              "Range (max-min)...16","Average (ug)...17","BC (µg)...18", "Adjusted_PM_dep_ug", "Adjusted_BC_dep_ug", 
                              "qc","...22","...23","...24","Filter ID...25",
                              "instrument","HHID", "Researcher ID","Date loaded into UPAS","Cartridge ID",
                              "A or E","...32", "...33", "...34", "Ambient location",
                              "PM_dep_ug", "BC_dep_ug", "...38", "...39", "Notes",
                              "...41", "grav Bs",      "BC Bs", "...44", "...45",
                              "...46", "...47", "...48", "...49", "...50",
                              "...51", "...52", "...53", "...54", "...55"))
  
  
  gravimetric_data <-
    dplyr::mutate(gravimetric_data,filterID = as.numeric(substr(filterID,3,5))) %>%
    dplyr::mutate(qc = gsub('y','good', qc, ignore.case = TRUE),
                  qc = gsub('n','bad', qc, ignore.case = TRUE)) %>%
    dplyr::filter(!is.na(filterID)) %>%
    dplyr::filter(!is.na(qc))
  
  
  return(gravimetric_data)
}

#a function to slice 5 digit of Beacon id, will add 0 in in front if less than n (n = 5) digits
id_slicer_add <- function(x, n){
  str2 = substr(x, nchar(x)-n+1, nchar(x))
  str2 = str_pad(str2,n,'left',pad = '0')
  return(str2)
}

#Convert ppm to mgm3
ppm_to_mgm3_function <- function(Conc_PPM_BGS,MolarMass,ambient_temp_K,pressure_atm,R,Mass_Conv){
  Vol_Conv <- 10^3 #L/m^3
  Mass_Conc_num <- Conc_PPM_BGS*10^-6*pressure_atm*MolarMass*Vol_Conv*Mass_Conv
  Mass_Conc_denom <- R*ambient_temp_K
  Mass_Conc <-Mass_Conc_num/Mass_Conc_denom
}


#Get AER.  Provide a segment of CO2 data, calculate and return the AER (the slope), correlation, and plot it.
AER_fun <- function(file,meta_data,raw_data_AER,output = 'meta_data'){
  
  raw_data_AER$time_hours <- (raw_data_AER$datetime-raw_data_AER$datetime[1])/3600 #Dif time in hours
  raw_data_AER$ln_CO2 <- log(raw_data_AER$CO2_ppm)
  meta_data$AERslope <- NA
  meta_data$AERr_squared <- NA
  
  #Plot data and save it
  meta_data <- tryCatch({ 
    lmfit <- lm(raw_data_AER$ln_CO2~raw_data_AER$time_hours)
    meta_data$AERslope <- lmfit$coefficients[2] 
    meta_data$AERr_squared <- summary(lmfit)$r.squared
    
    #Prepare some text for looking at the ratios of high to low temps.
    plot_name = gsub(".csv",".png",basename(file))
    plot_name = paste0("QA Reports/Instrument Plots/AER_",gsub(".csv",".png",plot_name))
    aerplot <- ggplot(raw_data_AER,aes(y = time_hours, x = ln_CO2)) +
      geom_point()+
      ggtitle(paste0('AER ',basename(file))) + 
      geom_smooth(method=lm) +
      theme(legend.title = element_blank())+
      theme_minimal() 
    print(aerplot)
    ggsave(filename=plot_name,plot=aerplot,width = 8, height = 6)
    return(meta_data)
  }, error = function(e) {
    print('Error calculating AER')
    return(meta_data)
  })
  
  return(meta_data)
}


ShiftTimeStamp_unops <- function(beacon_logger_data, newstartdatetime,timezone){
  
  #Number of seconds to shift the timestamps
  TimeShiftSeconds <- as.numeric(difftime(newstartdatetime, beacon_logger_data$datetime[1],units='secs'))
  
  #Shift the timestamps
  beacon_logger_data$datetime <- beacon_logger_data$datetime + TimeShiftSeconds
  beacon_logger_data
  #Get the timestamps in the right format.
  # beacon_logger_data[,datetime:=strftime(beacon_logger_data$datetime, "%Y-%m-%dT%H:%M:%S%Z", tz="GMT")]
  
}

baseline_correction_pats <- function(raw_data){
  #Get 10th percentile of first and last 20 points
  zeroprctile_initial <- quantile(raw_data$pm25_conc[1:20],c(0.1))
  zero_initial_time <- raw_data[1:20][pm25_conc==zeroprctile_initial,datetime][1]
  zeroprctile_final <- quantile(tail(raw_data$pm25_conc,20),c(0.1))
  zero_final_time <- tail(raw_data,20)[pm25_conc==zeroprctile_final,datetime][1]
  
  slope <- (zeroprctile_final - zeroprctile_initial)/as.numeric(difftime(zero_final_time,zero_initial_time,units='mins'))
  
  raw_data$minutecounter = as.numeric(difftime(raw_data$datetime,raw_data$datetime[1],units='mins'))
  raw_data$pm25_conc <- raw_data$pm25_conc - slope*raw_data$minutecounter
  raw_data$minutecounter = NULL
  return(raw_data)
}


#ambient analysis (pats, Lascar, UPAS)
ambient_analysis <- function(CO_calibrated_timeseries,pats_data_timeseries,upasmeta,gravimetric_data){
  ambient_pats_data_timeseries <- pats_data_timeseries["A"==substr(sampletype,1,1),]
  ambient_pats_data_timeseries$instrument <- 'PATS'
  ambient_data_realtime <- CO_calibrated_timeseries["A"==substr(sampletype,1,1),]
  ambient_data_realtime$instrument <- 'Lascar'
  # beacon_logger_COmerged = merge(beacon_logger_data,lascar_calibrated_subset, by.x = c("datetime","HHID","sampletype"),by.y = c("datetime","HHID","sampletype"), all.x = T, all.y = F)
  
  # ambient_data_realtime <- merge(ambient_data_realtime,ambient_pats_data_timeseries,by=c("datetime"),all.x = T, all.y = F)
  ambient_data_realtime <- melt(ambient_data_realtime, id.vars = c('datetime','instrument', 'sampletype','qc'), measure.vars = 'CO_ppm')
  meltedpats <- melt(ambient_pats_data_timeseries, id.vars = c('datetime','instrument', 'sampletype','qc'), measure.vars = c('pm25_conc')) %>%
    dplyr::mutate(variable = gsub('pm25_conc','PM µgm-3',variable))
  meltedpats_tempRH <- melt(ambient_pats_data_timeseries, id.vars = c('datetime','instrument', 'sampletype','qc'), measure.vars = c('degC_air','%RH_air')) %>%
    dplyr::mutate(class='met') %>%
    dplyr::mutate(instrument='PATS T/RH')
  ambient_data_realtime <- rbind(ambient_data_realtime,meltedpats)
  
  upasmeta$qc <- NULL
  ambient_grav <- merge(dplyr::filter(upasmeta,sampletype == "A"),gravimetric_data,by="filterID")
  ambient_grav$sampletype <- 'Ambient'
  ambient_grav$instrument <- 'UPAS'
  ambient_grav$flow_m3 <- ambient_grav$AverageVolumetricFlowRate/1000 * ambient_grav$RunTimeHrs*60 #flow rate in l/min*1m3/1000l, runtime h*60min/h
  ambient_grav$`PM µgm-3` <-  as.numeric(ambient_grav$PM_dep_ug)*1000000 / ambient_grav$flow_m3
  ambient_grav$`BC µgm-3` <-  as.numeric(ambient_grav$BC_dep_ug)*1000000 / ambient_grav$flow_m3
  ambient_grav$datetime <-  ambient_grav$datetime_start + ambient_grav$RunTimeHrs*60*60/2 #posixct is in seconds, so multiply hours by 3600 to get correct end date.
  meltedgrav <- melt(ambient_grav, id.vars = c('datetime','instrument', 'sampletype','qc'), measure.vars = c('PM µgm-3','BC_ugm3'))
  
  ambient_data <- rbind(ambient_data_realtime,meltedgrav) %>%
    dplyr::mutate(class = 'ambient')
  ambient_data <- rbind(ambient_data,meltedpats_tempRH)
  
  ggplot(ambient_data %>% filter(qc=='good'), aes_string(y = 'value', x = 'datetime', color = 'variable')) +
    geom_point(alpha = 0.4) +
    facet_wrap('instrument', ncol = 1, scales = "free") +
    theme_minimal() +
    theme(legend.position = "top") +
    ggtitle('Eldoret Kenya Background Ambient Data') +
    labs(y='',x='') +
    # scale_x_datetime(date_breaks = "2 day",date_labels = "%e-%b") +
    theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))
  
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/ambient_data.png",plot=last_plot(),dpi=200,device=NULL)
  saveRDS(ambient_data,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/ambient_data.rds")
  saveRDS(ambient_grav,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/ambient_grav.rds")
  writexl::write_xlsx(ambient_grav,"~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/ambient_grav.xlsx")
  
}


#Summary of items required for a deployment, and the different sources.
# fwrite(deployment, file="r_scripts/dummy_deployment_data.csv") 

deployment_check_fun <-  function(preplacement,equipment_IDs,tz,local_tz,meta_emissions,beacon_meta_qaqc,tsi_meta,lascar_meta,upasmeta){
  
  dummy_deployment <- fread("r_scripts/dummy_deployment_data.csv")
  # preplacement<-mobenzilist[[1]]
  base::message(preplacement$HHID)
  preplacement$HHID <- preplacement$HHIDnumeric
  preplacement$datestart <- as.Date(preplacement$UNOPS_Date,format = "%d-%m-%Y")
  #Which date in mobenzi to use???  Not clear!!!  Was using this: preplacement$Start
  start_time <- as.POSIXct(strftime(paste(as.Date(preplacement$UNOPS_Date,format = "%d-%m-%Y"),preplacement$DevicesONTime),tz=local_tz),tz=local_tz)
  
  #Prep the metadatastreams for merging.
  upasmetaT <- upasmeta
  colnames(upasmetaT) <- paste("upas", colnames(upasmetaT), sep = "_")
  upasmetaT$HHID <- upasmetaT$upas_HHID
  upasmetaT$datestart <- upasmetaT$upas_datestart
  upasmetaT <- dplyr::filter(upasmetaT,difftime(upas_datetime_start,start_time,units='mins')<1440 &
                               difftime(upas_datetime_start,start_time,units='mins')>0) #Keep only near samples
  
  tsi_metaT <- tsi_meta
  colnames(tsi_metaT) <- paste("tsi", colnames(tsi_metaT), sep = "_")
  tsi_metaT$HHID <- tsi_metaT$tsi_HHID
  tsi_metaT <- dplyr::filter(tsi_metaT,difftime(tsi_datetime_start,start_time,units='mins')<1440 &
                               difftime(tsi_datetime_start,start_time,units='mins')>0) #Keep only near samples that started after the personal sampling starttime
  lascar_metaT <- lascar_meta
  colnames(lascar_metaT) <- paste("lascar", colnames(lascar_metaT), sep = "_")
  lascar_metaT$HHID <- lascar_metaT$lascar_HHID
  lascar_metaT$sampletype <- lascar_metaT$lascar_sampletype
  lascar_metaT$datestart <- lascar_metaT$lascar_datestart
  lascar_metaT <- dplyr::filter(lascar_metaT,difftime(lascar_datetime_start,start_time,units='mins')>-1440 &
                                  difftime(lascar_metaT$lascar_datetime_start,start_time,units='mins')<1440) #Keep only near samples that started before the personal sampling starttime
  
  beacon_meta_qaqcT <- beacon_meta_qaqc
  colnames(beacon_meta_qaqcT) <- paste("beacon", colnames(beacon_meta_qaqcT), sep = "_")
  beacon_meta_qaqcT$HHID <- beacon_meta_qaqcT$beacon_HHID
  beacon_meta_qaqcT$sampletype <- beacon_meta_qaqcT$beacon_sampletype
  beacon_meta_qaqcT$datestart <- beacon_meta_qaqcT$beacon_datestart
  
  meta_emissionsT <- meta_emissions
  colnames(meta_emissionsT) <- paste("emissions", colnames(meta_emissionsT), sep = "_")
  meta_emissionsT$HHID <- meta_emissionsT$emissions_HHID
  meta_emissionsT$datestart <- meta_emissionsT$emissions_datetime_sample_start
  meta_emissionsT <- dplyr::filter(meta_emissionsT,abs(difftime(meta_emissionsT$emissions_datetimedecaystart,start_time,units='mins'))<1440) #Keep only near samples
  
  #Build deployment summaries
  deployment = tryCatch({ 
    
    #Merge streams
    upasqa_subset = merge(preplacement,upasmetaT,by='HHID')
    TSI_subset = merge(preplacement,tsi_metaT,by='HHID')
    Lascar_subset = merge(preplacement,lascar_metaT,by=c('HHID'))
    Beacon_subset = merge(preplacement,beacon_meta_qaqcT,by=c('HHID'))
    Emissions_subset = merge(preplacement,meta_emissionsT,by=c('HHID'))
    
    
    #Add flag summaries at some point!
    #Only grab the rows with the L and K sampletypes.
    Beacon_subsetK <- dplyr::filter(Beacon_subset,sampletype=='K')
    Beacon_subsetL <- dplyr::filter(Beacon_subset,sampletype=='L')
    Lascar_subsetK <- dplyr::filter(Lascar_subset,sampletype=='K')
    Lascar_subsetL <- dplyr::filter(Lascar_subset,sampletype=='L')
    
    deployment = data.frame(HHID = preplacement$HHID[1], 
                            startimemob = start_time)
    
    deployment$startdate_tsi = TSI_subset$tsi_datetime_start[1]
    deployment$tsiFile = TSI_subset$tsi_basename[1]
    deployment$BeaconLoggerKmob = preplacement$BeaconLoggerIDKitchen[1]
    deployment$BeaconLoggerKFile =  Beacon_subsetK$beacon_basename[1]
    deployment$BeaconLoggerLmob = preplacement$BeaconLoggerIDSecondary[1]
    deployment$BeaconLoggerLFile =  Beacon_subsetL$beacon_basename[1]
    deployment$PATSKmob = preplacement$PATSIDKitchen[1]
    deployment$PATSKEmXLSX = Emissions_subset$`emissions_PATS+ ID 1.5m`[1]
    # PATSKFile = 
    deployment$PATSLmob = preplacement$PATSIDSecondary[1]
    # PATSLFile = ,
    deployment$lascarKmob = preplacement$LascarIDKitchen[1]
    deployment$lascarKFile = Lascar_subsetK$lascar_basename[1]
    deployment$lascarKEmXLSX = Emissions_subset$`emissions_LASCAR ID 1.5m`[1]
    deployment$lascarLmob = preplacement$LascarIDSecondary[1]
    deployment$lascarLFile =  Lascar_subsetL$lascar_basename[1]
    deployment$upasIDFile = upasqa_subset$upas_basename[1]
    deployment$upasFilterFile = upasqa_subset$upas_filterID[1]
    deployment$upasFilterEmXLSX = Emissions_subset$upas_filterID[1]
    
    
    #If there are other sample types, manage them here, adding to the deployment dataframe.
    upasTSILascar_subsetC <- dplyr::filter(Lascar_subset,sampletype=='C')
    deployment$lascarCFile = upasTSILascar_subsetC$lascar_basename[1]
    deployment$lascarCmob = upasTSILascar_subsetC$lascar_loggerID[1]
    
    upasTSILascar_subset1 <- dplyr::filter(Lascar_subset,sampletype=='1')
    deployment$lascar1File = upasTSILascar_subset1$lascar_basename[1]
    deployment$lascar1EmXLSX = Emissions_subset$`emissions_LASCAR ID 1m`[1]
    
    upasTSILascar_subset2 <- dplyr::filter(Lascar_subset,sampletype=='2')
    deployment$lascar2File = upasTSILascar_subset2$lascar_basename[1]
    deployment$lascar2EmXLSX = Emissions_subset$`emissions_Lascar ID 2m`[1]
    deployment
  }, error = function(error_condition) {
    #add "fake" meta_data
    base::message("Error building deployment")
    deployment <- dummy_deployment
    deployment$HHID <- preplacement$HHID
    deployment$datetime_start <- start_time
    deployment})
}

sampletype_fix_function <- function(raw_data){
  raw_data <- dplyr::mutate(as.data.frame(raw_data),
                            sampletype =    case_when(
                              sampletype == "A"~"Ambient",
                              sampletype == "A2"~"Ambient Dup",
                              sampletype == "K"~"Kitchen",
                              sampletype == "L"~"Living Room",
                              sampletype == "C"~"Cook",
                              sampletype == "1"~"1m",
                              sampletype == "2"~"2m",
                              sampletype == "L2"~"Living Room Dup",
                              sampletype == "K2"~"Kitchen Dup",
                              TRUE ~ sampletype)) %>% as.data.table()
  return(raw_data)
}

sampletype_fix_function_beacon <- function(raw_data){
  raw_data <- dplyr::mutate(as.data.frame(raw_data),
                            location_nearest =    case_when(
                              location_nearest == "A"~"Ambient",
                              location_nearest == "A2"~"Ambient Dup",
                              location_nearest == "K"~"Kitchen",
                              location_nearest == "L"~"Living Room",
                              location_nearest == "C"~"Cook",
                              location_nearest == "1"~"1m",
                              location_nearest == "2"~"2m",
                              location_nearest == "L2"~"Living Room Dup",
                              location_nearest == "K2"~"Kitchen Dup",
                              TRUE ~ location_nearest),
                            location_kitchen_threshold =    case_when(
                              location_kitchen_threshold == "A"~"Ambient",
                              location_kitchen_threshold == "A2"~"Ambient Dup",
                              location_kitchen_threshold == "K"~"Kitchen",
                              location_kitchen_threshold == "L"~"Living Room",
                              location_kitchen_threshold == "C"~"Cook",
                              location_kitchen_threshold == "1"~"1m",
                              location_kitchen_threshold == "2"~"2m",
                              location_kitchen_threshold == "L2"~"Living Room Dup",
                              location_kitchen_threshold == "K2"~"Kitchen Dup",
                              TRUE ~ location_kitchen_threshold),
                            location_kitchen_threshold80 =    case_when(
                              location_kitchen_threshold80 == "A"~"Ambient",
                              location_kitchen_threshold80 == "A2"~"Ambient Dup",
                              location_kitchen_threshold80 == "K"~"Kitchen",
                              location_kitchen_threshold80 == "L"~"Living Room",
                              location_kitchen_threshold80 == "C"~"Cook",
                              location_kitchen_threshold80 == "1"~"1m",
                              location_kitchen_threshold80 == "2"~"2m",
                              location_kitchen_threshold80 == "L2"~"Living Room Dup",
                              location_kitchen_threshold80 == "K2"~"Kitchen Dup",
                              TRUE ~ location_kitchen_threshold80)) %>% 
    as.data.table()
  return(raw_data)
}

plot_deployment <- function(selected_preplacement,beacon_logger_data,pats_data_timeseries,
                            CO_calibrated_timeseries,tsi_timeseries,ecm_dot_data){
  
  tryCatch({
    # selected_preplacement <- preplacement[i,]
    HHIDselected <- selected_preplacement$HHIDnumeric
    mindatetime <- selected_preplacement$start_datetime
    maxdatetime <- mindatetime + 24*60*60
    
    #Use ECM start and stop time to truncate the data.
    maxdatetimeCO <- CO_calibrated_timeseries[CO_ppm>-1 & HHID %in% HHIDselected,lapply(.SD,max),.SDcols="datetime"]
    
    selected_COppm <- CO_calibrated_timeseries[CO_ppm>-1 & HHID %in% HHIDselected,c("CO_ppm","datetime","sampletype","emission_tags","qc")]
    selected_COppm<-sampletype_fix_function(selected_COppm)
    
    selected_pats <- pats_data_timeseries[HHID %in% HHIDselected,c("pm25_conc","datetime","sampletype","emission_tags","qc")]
    selected_pats<-sampletype_fix_function(selected_pats)
    
    selected_ecm <- ecm_dot_data[HHID %in% HHIDselected,c("pm25_conc","datetime","sampletype")]
    
    selected_beacon <- beacon_logger_data[HHID %in% HHIDselected,c("location_nearest","location_kitchen_threshold","datetime","nearest_RSSI")]
    # selected_beacon<-sampletype_fix_function(selected_beacon)
    
    selected_tsi <- tsi_timeseries[HHID %in% HHIDselected,c("datetime","loggerID","HHID","qc","emission_tags","CO_ppm","CO2_ppm")]
    
    p1 <- ggplot(aes(y = CO_ppm, x = datetime), data = selected_COppm) + 
      geom_point(aes(colour = sampletype, shape = qc), alpha=0.25) + 
      theme_bw(10) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_x_datetime(limits=c(mindatetime,maxdatetime)) +
      scale_y_continuous(limits = c(0,300))   +
      ggtitle(paste0("HHID KE",HHIDselected)) + 
      ylab("CO ppm") 
    
    p2 <- ggplot(aes(y = pm25_conc, x = datetime), data = selected_pats) +
      geom_point(aes(colour = sampletype, shape = qc), alpha=0.25) +
      theme_bw(10) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_x_datetime(limits=c(mindatetime,maxdatetime)) +
      scale_y_continuous(limits = c(0,3000)) +
      ylab("PATS+ ugm-3")
    
    p3 <- ggplot(aes(y = pm25_conc, x = datetime), data = selected_ecm) +
      geom_point(aes(colour = sampletype, shape = qc), alpha=0.25) +
      theme_bw(10) +
      scale_x_datetime(limits=c(mindatetime,maxdatetime)) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_y_continuous(limits = c(0,1000)) +
      ylab("ECM/MicroPEM ugm-3")
    
    p4 <- ggplot(aes(y = nearest_RSSI, x = datetime), data = selected_beacon) +
      geom_point(aes(colour = location_nearest), alpha=0.25)+
      theme_bw(10) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_x_datetime(limits=c(mindatetime,maxdatetime)) +
      # scale_y_continuous(limits = c(0,3000)) +
      ylab("Localization")
    
    p5 <- ggplot(data = selected_tsi,aes(y = CO_ppm, x = datetime, shape = emission_tags, colour = emission_tags,group = qc), alpha=0.25) +
      geom_point()+
      theme_bw(10) +
      scale_y_log10() +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_x_datetime(limits=c(mindatetime,maxdatetime)) +
      ylab("TSI CO2 and CO ppm")
    
    plot_name = paste0("QA Reports/Instrument Plots/Combo_",HHIDselected,".png")
    
    # if(!file.exists(plot_name)){
    png(plot_name,width = 1000, height = 800, units = "px")
    
    
    egg::ggarrange(p1, p2,p4,p5, heights = c(0.25, 0.25,.25,.25))
    
    dev.off()
    
    # }
  }, error = function(error_condition) {
    print(paste0('Errore in HHID ', HHIDselected,', index ',i))
  }
  , finally={})
}


plot_deployment_merged <- function(all_merged_temp){
  # all_merged_temp <- all_merged[HHID == uniqueHHIDs[i]]
  tryCatch({
    
    all_merged_temp_ecm <- pivot_longer(all_merged_temp,
                                        cols = starts_with("PM25"),
                                        names_to = "PM25",
                                        names_prefix = "PM25",
                                        values_to = "values",
                                        values_drop_na = TRUE)
    
    p1pm <- all_merged_temp_ecm %>% filter(PM25 == 'Cook' | PM25 == 'Kitchen') %>%
      ggplot(aes(y = values, x = datetime)) + 
      geom_point(aes(colour = PM25), alpha=0.25) + 
      theme_bw(10) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_y_continuous(limits = c(0,1000))   +
      # theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))+
      ggtitle(all_merged_temp$HHID[1]) + 
      ylab("ECM ugm-3") 
    
    p2pm <- pivot_longer(all_merged_temp,
                         cols = starts_with("PATS"),
                         names_to = "PATS",
                         names_prefix = "PATS",
                         values_to = "values",
                         values_drop_na = TRUE) %>%
      mutate(PATS = gsub('_',' ',PATS)) %>%
      ggplot(aes(y = values, x = datetime)) +
      geom_point(aes(colour = PATS), alpha=0.25) +
      theme_bw(10) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_y_continuous(limits = c(10,1000)) +
      # theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))+
      ylab("PATS ugm-3")
    
    p3pm <- all_merged_temp_ecm %>% filter(PM25 != 'Cook' & PM25 != 'Kitchen') %>%
      ggplot(aes(y = values, x = datetime)) + 
      geom_point(aes(colour = PM25), alpha=0.25) + 
      theme_bw(10) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_y_continuous(limits = c(0,1000))   +
      # theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))+
      ggtitle(all_merged_temp$HHID[1]) + 
      ylab("Indirect ugm-3") 
    
    all_merged_temp_beacon <- pivot_longer(all_merged_temp,
                                           cols = starts_with("location"),
                                           names_to = "location",
                                           names_prefix = "location",
                                           values_to = "values",  
                                           values_drop_na = TRUE) %>%
      mutate(location = gsub('_',' ',location),
             locationvalues = location)
    levels(all_merged_temp_beacon$locationvalues) = 1:length(unique(all_merged_temp_beacon$locationvalues))
    
    p4 <- ggplot(aes(y = locationvalues, x = datetime), data = all_merged_temp_beacon) +
      geom_tile(aes(colour = values,fill=values ), alpha=0.25) +
      theme_bw(10) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      # theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))+
      ylab("localization by approach")
    
    
    all_merged_temp_sums <- pivot_longer(all_merged_temp,
                                         cols = starts_with("sums"),
                                         names_to = "sums",
                                         names_prefix = "sums",
                                         values_to = "values",
                                         values_drop_na = TRUE) %>%
      mutate(sums = gsub('_',' ',sums),
             sumsvalues = sums)
    levels(all_merged_temp_sums$sumsvalues) = 1:length(unique(all_merged_temp_sums$sumsvalues))
    
    p5 <- ggplot(aes(y = as.factor(sums), x = datetime), data = all_merged_temp_sums) +
      geom_tile(aes(colour = values,fill=values), alpha=0.25) +
      theme_set(theme_bw(10) + theme(legend.background=element_blank())) +
      theme(legend.background=element_blank()) +
      
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      # scale_y_continuous(limits = c(20,100)) +
      # theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))+
      ylab("Cooking indicator")
    
    
    
    plot_name = paste0("QA Reports/Instrument Plots/all_merged_pm_",all_merged_temp$HHID[1],".png")
    png(plot_name,width = 1000, height = 800, units = "px")
    egg::ggarrange(p1pm, p2pm,p3pm,p4,p5, heights = c(0.2,0.2, 0.2,.2,.2))
    dev.off()
    
    
    #### Create CO plot
    all_merged_temp_co <- pivot_longer(all_merged_temp,
                                       cols = starts_with("CO"),
                                       names_to = "CO",
                                       names_prefix = "CO",
                                       values_to = "values",
                                       values_drop_na = TRUE) %>%
      mutate(CO = gsub('_','',CO)) %>%
      mutate(CO = gsub('ppm','',CO)) 
    
    
    p1co <- all_merged_temp_co %>% filter(CO == 'Cook' | CO == 'Kitchen' | CO == 'LivingRoom') %>%
      ggplot(aes(y = values, x = datetime)) + 
      geom_point(aes(colour = CO), alpha=0.25) + 
      theme_bw(10) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_y_continuous(limits = c(0,100))   +
      # theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))+
      ggtitle(all_merged_temp$HHID[1]) + 
      ylab("CO ppm") 
    
    
    p2co <- all_merged_temp_co %>% filter(CO != 'Cook' & CO != 'Kitchen' & CO != 'LivingRoom') %>%
      ggplot(aes(y = values, x = datetime)) + 
      geom_point(aes(colour = CO), alpha=0.25) + 
      theme_bw(10) +
      theme(legend.title=element_blank(),axis.title.x = element_blank()) +
      scale_y_continuous(limits = c(0,100))   +
      # theme(axis.text.x = element_text(angle = 30, hjust = 1,size=10))+
      ggtitle(all_merged_temp$HHID[1]) + 
      ylab("CO ppm indirect") 
    
    plot_name = paste0("QA Reports/Instrument Plots/all_merged_co_",all_merged_temp$HHID[1],".png")
    png(plot_name,width = 1000, height = 800, units = "px")
    egg::ggarrange(p1co, p2co,p4,p5, heights = c(0.25,0.25, 0.25,.25))
    dev.off()
    
    # }
  }, error = function(error_condition) {
    print(paste0('Errore in HHID ', HHIDselected,', index ',i))
  }
  , finally={})
}


emailgroup <-  function(todays_date){
  sender <- "beaconnih@gmail.com"
  recipients <- c("rpiedrahita@berkeleyair.com","mrossanese@berkeleyair.com")
  send.mail(from = sender,
            to = recipients,
            subject = paste0("UNOPS/CAA QA Report ",as.character(Sys.Date())),
            body = "UNOPS/CAA data report (new format). Please correct filenames as needed, and note any devices that need maintenance. 
          The Deployment Summary worksheet has information from the filenames, Mobenzi, and Excel emissions database.  These should be cross-referenced and disparities investigated!",
            smtp = list(host.name = "smtp.gmail.com", port = 465, 
                        user.name = "beaconnih@gmail.com",            
                        passwd = "CookaBLE99", ssl = TRUE),
            authenticate = TRUE,
            attach.files = c(paste0("QA Reports/QA report", "_", todays_date, ".xlsx")),
            send = TRUE)
}

#Not done - need to truncate the ends.
truncate_timeseries <- function(raw_data,startdatetime,enddatetime){
  raw_data <- raw_data[datetime>startdatetime,]# & datetime<enddatetime)
}


#Add tag to data based on mobenzi info
#Good.
tag_timeseries_mobenzi <- function(raw_data,preplacement,filename){
  #Get the relevant preplacement row, based on HHID and start date.
  raw_data[,ecm_tags:="collecting"]
  
  if(raw_data$sampletype[1] %in% c('C','L','K','C2','L2','K2')){
    preplacement$HHID <- preplacement$HHIDnumeric
    preplacement_matched <- merge(raw_data[1,],preplacement, by="HHID") %>%
      dplyr::filter(datetime-start_datetime<1)
    ECM_end = preplacement_matched$start_datetime+86400
    
    if(dim(preplacement_matched)[1]>0){ #If there is a match
      raw_data[,ecm_tags := ifelse(datetime>preplacement_matched$start_datetime & datetime<ECM_end,'deployed',ecm_tags)]
      if(abs(raw_data$datetime[1]-max(raw_data$datetime,na.rm=TRUE))>2){      
        raw_data[,ecm_tags := ifelse(datetime > ECM_end,'intensive',ecm_tags)]
      }
    } else {
      print(paste('Error mobenzi-tagging ', filename$basename))
    }
  }
  raw_data
}


#Add tag to data based on emissions database info
tag_timeseries_emissions <- function(raw_data,meta_emissions,meta_data,filename){
  #Get the relevant preplacement row, based on HHID and start date.
  raw_data[,emission_tags:="collecting"]
  
  meta_matched <- dplyr::left_join(meta_data,meta_emissions,by="HHID") %>% #in case of repeated households, keep the nearest one
    dplyr::filter(min(abs(difftime(datetime_start,datetimedecaystart,units='days')))==abs(difftime(datetime_start,datetimedecaystart,units='days')))
  
  if(dim(meta_matched)[1]>0){
    raw_data[,'emission_tags'][raw_data$datetime %between% c(meta_matched$datetime_sample_start,meta_matched$datetime_BGf_start)] ="cooking"
    raw_data[,'emission_tags'][raw_data$datetime %between% c(meta_matched$datetime_BGi_start,meta_matched$datetime_sample_start) ] ="BG_initial"
    raw_data[,'emission_tags'][raw_data$datetime %between% c(meta_matched$datetime_sample_end,meta_matched$datetime_BGf_start)] ="BG_final"
    raw_data[,'emission_tags'][raw_data$datetime %between% c(meta_matched$datetimedecaystart,meta_matched$datetimedecayend)] ="Decay"
    
  } else if(raw_data$sampletype[1] %in% 'A'){
    print(paste('Ambient file found ', filename$basename))
  } else {
    print(paste('Error emissions-tagging, no meta_data matched ', filename$basename))
  }
  return(raw_data)
}


#Get start and stop times of ECM files to use in the deployments
update_preplacement <- function(raw_data,preplacement){
  #If it is a kitchen or cook's ECM
  if(raw_data$sampletype[1] %in% c('C','K','K2')){
    raw_data_temp <- raw_data[c(1,.N),.(datetime,HHID)]
    raw_data_temp$UNOPS_Date <- as.Date(raw_data_temp$datetime, "%Y-%m-%d")
    raw_data_temp$UNOPS_Date <-format(raw_data_temp$UNOPS_Date, "%d-%m-%Y")   
    
    #Get index of matching preplacement row
    preplacement_test <- merge(preplacement,raw_data_temp[1,], by=c("HHID","UNOPS_Date")) %>%
      dplyr::filter(datetime-start_datetime<1)
    preplacement$ECM_start_k
    preplacement_test
    
    
    
  }
}




