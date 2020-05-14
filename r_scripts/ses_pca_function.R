
ses_function <- function(mobenzi_rapid){
  
  #Prep the categorical variables and reshape to make them into numeric/binary values.
  #The binary variables are given a value of 1 for 'having something', and 0 for not having something, for consistency.
  rapid_ses_categorical<-   dplyr::mutate(mobenzi_rapid, id = paste(HHID,Phone)) %>%
    dplyr::select(c('id','EducationHighest',#'HeadofHouseholdIncome',
                    # 'C3_Farmsownland(whetherthatlandisownedorrented)','C3_Daylabourer(farminganotherperson’sland,builder,dailyworkeretc.)',
                    # 'C3_Governmentemployee(doctor,nurse,police,teacheretc.)','C3_Employeeinabusiness(Factory/industrialworker,worksinashop,receptionist,securityguard,etc.)','C3_Hasownbusiness(businessman,ownsashop,traderetc.)','C3_Craftsperson(tailor,carpenter,seamstressetc.)','C3_Runthehousehold/Careforfamily','C3_Retired','C3_Othertypeofjob','C3_Currentlyunemployed/nothing',
                    # 'Income','WaterAccessYN',
                    'WaterSource',
                    # 'MostUsedCookstove',#'MostUsedFuel','D6_Electricity','D6_Kerosene','D6_Cookinggas/LPG','D6_Charcoalunprocessed','D6_Charcoalbriquettes/pellets','D6_Wood','D6_Agriculturalorcropresidue/grass/',
                    # 'D6_straw/shrubs/corncobs','D6_Processedbiomasspellets/briquettes','D6_Woodchips','D6_Sawdust','D6_Animalwaste/dung','D6_Garbage/plastic','D6_None',
                    # 'D8_Electricity','D8_Kerosene','D8_Cookinggas/LPG','D8_Charcoalunprocessed','D8_Charcoalbriquettes/pellets','D8_Wood','D8_Agriculturalorcropresidue/grass/','D8_straw/shrubs/corncobs','D8_Processedbiomasspellets/briquettes','D8_Woodchips','D8_Sawdust','D8_Animalwaste/dung','D8_Garbage/plastic','DoYouPayForFuel',
                    #'D12_Electricity','D12_Kerosene','D12_Cookinggas/LPG','D12_Charcoalunprocessed','D12_Charcoalbriquettes/pellets','D12_Wood','D12_Agriculturalorcropresidue/grass/','D12_straw/shrubs/corncobs','D12_Processedbiomasspellets/briquettes','D12_Woodchips','D12_Sawdust','D12_Animalwaste/dung','D12_Garbage/plastic',
                    # 'WhereCook','SharedKitchenYN','HeatingDeviceEver',
                    'LightingSource',#'HeaterType',#'NumberCylindersAtHome',#'LPGRefillsPerYear',
                    'FloorMaterial','RoofingMaterial','WallMaterial')) %>%
    gather(Key,value,-id) %>%
    dplyr::group_by(id, value) %>%
    dplyr::summarise(count = n())  %>%
    spread(value, count, fill = 0) %>%
    as.data.frame() 
  
  #Prep the binary variables.  Join with the categorical ones.
  rapid_ses_binary<-   dplyr::mutate(mobenzi_rapid, id = paste(HHID,Phone)) %>%
    dplyr::select(c('id','OwnorRent',
                    'C6_Animal(s)(cows,sheep,goatsetc.)','C6_Cellphone','C6_Smartphone','C6_Radio','C6_Hi-Fi/CD-player','C6_Solarconnection','C6_ElectricityConnection','C6_TV','C6_SatelliteTV','C6_Refrigerator/fridge/freezer','C6_Shower/bathwithinhouse','C6_Land','C6_Bicycle','C6_Moped/Motorcycle','C6_Pick-uptruck','C6_Car','C6_Computer','C6_Washingmachine',
                    'C6_Tractor',
                    'SepticorFlushingToiletInside','LatrineinCompound','UseLPG'
                    #'MostUsedFuel','D6_Electricity','D6_Kerosene','D6_Cookinggas/LPG','D6_Charcoalunprocessed','D6_Charcoalbriquettes/pellets','D6_Wood','D6_Agriculturalorcropresidue/grass/',
                    # 'D6_straw/shrubs/corncobs','D6_Processedbiomasspellets/briquettes','D6_Woodchips','D6_Sawdust','D6_Animalwaste/dung','D6_Garbage/plastic','D6_None',
                    # 'D8_Electricity','D8_Kerosene','D8_Cookinggas/LPG','D8_Charcoalunprocessed','D8_Charcoalbriquettes/pellets','D8_Wood','D8_Agriculturalorcropresidue/grass/','D8_straw/shrubs/corncobs','D8_Processedbiomasspellets/briquettes','D8_Woodchips','D8_Sawdust','D8_Animalwaste/dung','D8_Garbage/plastic','DoYouPayForFuel',
                    #'D12_Electricity','D12_Kerosene','D12_Cookinggas/LPG','D12_Charcoalunprocessed','D12_Charcoalbriquettes/pellets','D12_Wood','D12_Agriculturalorcropresidue/grass/','D12_straw/shrubs/corncobs','D12_Processedbiomasspellets/briquettes','D12_Woodchips','D12_Sawdust','D12_Animalwaste/dung','D12_Garbage/plastic',
                    # 'WhereCook','SharedKitchenYN','HeatingDeviceEver',
    )) %>%
    dplyr::mutate(SepticorFlushingToiletInside = as.numeric(as.factor(SepticorFlushingToiletInside))-1) %>%
    dplyr::mutate(OwnorRent = abs(as.numeric(as.factor(OwnorRent))-2)) %>%
    dplyr::mutate(LatrineinCompound = as.numeric(as.factor(LatrineinCompound))-1) %>%
    dplyr::mutate(UseLPG = as.numeric(as.factor(UseLPG))-1) %>%
    # dplyr::left_join(rapid_ses_categorical,by='id') %>%
    # dplyr::select(-id,-wall_NA,-roofing_NA,-Other,-`floor_Other, specify`) %>%
    dplyr::select(-id) %>%
    dplyr::mutate_if(is.character,as.numeric) %>%
    setNames(gsub('C6_','',names(.)))
  
  row.names(rapid_ses_binary) <- paste0(mobenzi_rapid$HHID,'*',mobenzi_rapid$Phone)
  
  
  png(filename='Results/SES PCA/SES_corrgram.png',width = 550, height = 480, res = 100)
  corrgram(rapid_ses_binary, order=NULL, lower.panel=panel.shade,
           upper.panel=NULL, text.panel=panel.txt,
           main="SES Asset Correlations")
  dev.off()
  
  
  #Run PCA on the data.
  ses.pca <- prcomp(rapid_ses_binary, center = TRUE,scale. = TRUE)
  # summary(ses.pca)
  png(filename='Results/SES PCA/PCA_scree.png',width = 550, height = 480, res = 100)
  fviz_eig(ses.pca)
  dev.off()
  
  #Get SES prediction from the first PC.  Large negative is rich, large positive is poor.
  predicted_ses <- data.frame(score = predict(ses.pca, rapid_ses_binary)[,1]) %>%
    mutate(quantile = ntile(score, 5),  #Split into quintiles
    id = paste(mobenzi_rapid$HHID,'*',mobenzi_rapid$Phone),
    HHID = mobenzi_rapid$HHID
  )
  
  predicted_summarystats <- dplyr::mutate(rapid_ses_binary,
                                          pca_score=predicted_ses$score,
                                          pca_quantile = predicted_ses$quantile) %>%
    dplyr::group_by(pca_quantile) %>%
    summarise_all(list(mean = mean, stdev = sd)) %>%
    write.xlsx('Results/SES PCA/PCA_AssetIndex.xlsx')
  
  
  png(filename='Results/SES PCA/PCA_biplot.png',width = 550, height = 480, res = 100)
  ggbiplot(ses.pca)
  dev.off()
  
  return(predicted_ses)
}
