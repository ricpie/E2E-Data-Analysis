#Analyze intensive samples (day-day variability/compare)

intensive_analysis = function(all_merged_intensive,all_merged_summary,preplacement,meta_emissions){
  
  # Calculate daily averages.  Get N for each.  Keep only the ones with 1200 or more minutes
  intensive = all_merged_intensive %>% select(-Date,-stovetype) %>%
    dplyr::left_join(meta_emissions %>%
                       dplyr::select(HHID_full,`Start time (Mobenzi Pre-placement)-- 1`) %>%
                       dplyr::mutate(HHID = HHID_full,
                                     Date = as.Date(`Start time (Mobenzi Pre-placement)-- 1`)),
                     by = c('HHID')) %>%
    group_by(HHID,Date) %>%
    arrange(datetime) %>%
    mutate(row_number = dplyr::row_number(),
           day = case_when(row_number<1440 ~ 1,
                           row_number>1440 & row_number<2880 ~ 2,
                           row_number>2880 & row_number<4320 ~ 3,
                           row_number>4320 & row_number<5760 ~ 4,
                           row_number>5760 & row_number<7200 ~ 5,
                           row_number>7200 ~ 6)
    ) %>%
    group_by(HHID,day,Date) %>%
    add_count() %>%
    filter(n>1200) %>%
    dplyr::summarise(Intensive_meanPM25Kitchen = mean(PATS_Kitchen,na.rm = TRUE),
                     # Intensive_sdPM25Kitchen = sd(PATS_Kitchen,na.rm = TRUE),
                     Intensive_N_PM25Kitchen = sum(!is.na(PATS_Kitchen)),
                     Intensive_meanPM25LivingRoom = mean(PATS_LivingRoom,na.rm = TRUE),
                     Intensive_N_PM25LivingRoom = sum(!is.na(PATS_LivingRoom)),
                     Intensive_meanCO_ppmKitchen = mean(CO_ppmKitchen,na.rm = TRUE),
                     # Intensive_N_CO_ppmKitchen = sum(!is.na(CO_ppmKitchen)),
                     Intensive_meanPM25indirect = mean(pm25_conc_beacon_nearest_ecm,na.rm = TRUE),
                     Intensive_N_PM25indirect = sum(!is.na(pm25_conc_beacon_nearest_ecm)),
                     # Intensive_N_CO_ppmLivingRoom = sum(!is.na(CO_ppmLivingRoom)),
                     Intensive_sum_TraditionalManufactured_minutes = sum(sumstraditional_manufactured,na.rm = TRUE),
                     Intensive_N_sumstraditional_manufactured = sum(!is.na(sumstraditional_manufactured)),
                     Intensive_sum_traditional_non_manufactured = sum(sumstraditional_non_manufactured,na.rm = TRUE),
                     Intensive_N_sum_traditional_non_manufactured = sum(!is.na(sumstraditional_non_manufactured)),
                     Intensive_sum_lpg = sum(sumslpg,na.rm = TRUE),
                     Intensive_N_sum_lpg = sum(!is.na(sumslpg))
    ) %>%      
    group_by(HHID) %>%
    add_count() %>%
    dplyr::left_join(meta_emissions %>%
                       dplyr::select(HHID_full,stovetype) %>%
                       dplyr::mutate(HHID = HHID_full),
                     by = c('HHID')) %>%
    mutate(stovetype = factor(stovetype, levels=c("LPG", "Charcoal", "Trad Biomass")))
  
  
  
  
  kitchen_cov_by_day = intensive %>% 
    filter(n>1) %>%
    filter(Intensive_N_PM25Kitchen>1200) %>%
    distinct(HHID,day, .keep_all = TRUE)  %>%
    group_by(stovetype) %>%
    mutate(n_hhid = n_distinct(HHID)) %>%
    filter(day<5) %>%
    mutate(meanday_4 = mean(Intensive_meanPM25Kitchen,na.rm = T),
           sdday_4 = sd(Intensive_meanPM25Kitchen,na.rm = T),
           covday_4 = sdday_4/meanday_4*100,
           nday4 = n()) %>%
    filter(day<4) %>%
    mutate(meanday_3 = mean(Intensive_meanPM25Kitchen,na.rm = T),
           sdday_3 = sd(Intensive_meanPM25Kitchen,na.rm = T),
           covday_3 = sdday_3/meanday_3*100)  %>%
    filter(day<3) %>%
    mutate(meanday_2 = mean(Intensive_meanPM25Kitchen,na.rm = T),
           sdday_2 = sd(Intensive_meanPM25Kitchen,na.rm = T),
           covday_2 = sdday_2/meanday_2*100) %>%
    filter(day<2) %>%
    mutate(meanday_1 = mean(Intensive_meanPM25Kitchen,na.rm = T),
           sdday_1 = sd(Intensive_meanPM25Kitchen,na.rm = T),
           covday_1 = sdday_1/meanday_1*100) %>%
    distinct(HHID, .keep_all = TRUE) %>%
    select(-day) %>%
    ungroup()
  
  
  joiners = kitchen_cov_by_day %>%
    gather(key = "meanday_", value = "meanday", meanday_1, meanday_2,meanday_3) %>%
    gather(key = "covday_", value = "covday", covday_1, covday_2,covday_3) %>%
    gather(key = "sdday_", value = "sdday", sdday_1, sdday_2,sdday_3) %>%
    filter(substring(meanday_, 8) == substring(covday_, 7)) %>%
    filter(substring(meanday_, 8) == substring(sdday_, 6))  %>%
    distinct(stovetype,meanday_, .keep_all = TRUE)  %>%
    mutate(day = gsub('covday_','',covday_)) %>% ungroup()
  
  
  
  #Plot COV and means/sd
  meanColor <- "#69b3a2"
  COVColor <- rgb(0.2, 0.6, 0.9, 1)  
  coeff = .1
  pd<-position_dodge(.9)
  
  
  gg_cov_kitchen <-  joiners %>%
    ggplot(aes(x=day,y = meanday)) + 
    geom_point(color = meanColor) +
    # geom_line(color = meanColor) +
    geom_point(aes(x=day,y=covday/coeff),size=2,color = COVColor,position = pd) +
    geom_line(aes(x=day,y=covday/coeff),color = COVColor,position = pd) +
    labs(x="Successive days monitoring") +
    theme_minimal(base_size = 16)+
    geom_errorbar(aes(ymin=meanday-sdday, ymax=meanday+sdday), width=.1,color = meanColor,position = pd) +
    facet_wrap(~stovetype, ncol = 1) + 
    scale_y_continuous(
      # Features of the first axis
      name = expression("Kitchen ", paste(PM[2.5], " (", mu, "g ", m^-3,")")),
      # Add a second axis and specify its features
      sec.axis = sec_axis(~.*coeff, name="Coefficient of variation (%)")
    ) + 
    theme(
      axis.title.y = element_text(color = meanColor, size=13),
      axis.title.y.right = element_text(color = COVColor, size=13)
    ) 
  
  ggsave("Results/kitchen_means_intensive.png",plot=gg_cov_kitchen,width = 8, height = 6,dpi=300,device=NULL)
  
  
  #### Assess COV, etc. for indirect exposure estimates
  intensive_indirect = all_merged_intensive %>% select(-Date,-stovetype) %>%
    dplyr::left_join(meta_emissions %>%
                       dplyr::select(HHID_full,`Start time (Mobenzi Pre-placement)-- 1`) %>%
                       dplyr::mutate(HHID = HHID_full,
                                     Date = as.Date(`Start time (Mobenzi Pre-placement)-- 1`)),
                     by = c('HHID')) %>%
    group_by(HHID,Date) %>%
    arrange(datetime) %>%
    filter(!is.na(pm25_conc_beacon_nearest_ecm)) %>%
    mutate(row_number = dplyr::row_number(),
           day = case_when(row_number<1440 ~ 1,
                           row_number>1440 & row_number<2880 ~ 2,
                           row_number>2880 & row_number<4320 ~ 3,
                           row_number>4320 & row_number<5760 ~ 4,
                           row_number>5760 & row_number<7200 ~ 5,
                           row_number>7200 ~ 6)
    ) %>%
    group_by(HHID,day,Date) %>%
    add_count() %>%
    filter(n>1200) %>%
    dplyr::summarise(
      Intensive_meanPM25indirect = mean(pm25_conc_beacon_nearest_ecm,na.rm = TRUE),
      Intensive_N_PM25indirect = sum(!is.na(pm25_conc_beacon_nearest_ecm))
    ) %>%      
    group_by(HHID) %>%
    add_count() %>%
    dplyr::left_join(meta_emissions %>%
                       dplyr::select(HHID_full,stovetype) %>%
                       dplyr::mutate(HHID = HHID_full),
                     by = c('HHID')) %>%
    mutate(stovetype = factor(stovetype, levels=c("LPG", "Charcoal", "Trad Biomass")))
  
  indirect_cov_by_day = intensive_indirect %>% 
    filter(n>1) %>%
    filter(Intensive_N_PM25indirect>1200) %>%
    distinct(HHID,day, .keep_all = TRUE)  %>%
    group_by(stovetype) %>%
    filter(day<5) %>%
    mutate(n_hhid = n_distinct(HHID)) %>%
    mutate(meanday_4 = mean(Intensive_meanPM25indirect,na.rm = T),
           sdday_4 = sd(Intensive_meanPM25indirect,na.rm = T),
           covday_4 = sdday_4/meanday_4*100,
           nday4 = n()) %>%
    filter(day<4) %>%
    mutate(meanday_3 = mean(Intensive_meanPM25indirect,na.rm = T),
           sdday_3 = sd(Intensive_meanPM25indirect,na.rm = T),
           covday_3 = sdday_3/meanday_3*100)  %>%
    filter(day<3) %>%
    mutate(meanday_2 = mean(Intensive_meanPM25indirect,na.rm = T),
           sdday_2 = sd(Intensive_meanPM25indirect,na.rm = T),
           covday_2 = sdday_2/meanday_2*100) %>%
    filter(day<2) %>%
    mutate(meanday_1 = mean(Intensive_meanPM25indirect,na.rm = T),
           sdday_1 = sd(Intensive_meanPM25indirect,na.rm = T),
           covday_1 = sdday_1/meanday_1*100) %>%
    distinct(HHID, .keep_all = TRUE) %>%
    select(-day) %>%
    ungroup()
  
  
  
  joiners = indirect_cov_by_day %>%
    gather(key = "meanday_", value = "meanday", meanday_1, meanday_2,meanday_3) %>%
    gather(key = "covday_", value = "covday", covday_1, covday_2,covday_3) %>%
    gather(key = "sdday_", value = "sdday", sdday_1, sdday_2,sdday_3) %>%
    filter(substring(meanday_, 8) == substring(covday_, 7)) %>%
    filter(substring(meanday_, 8) == substring(sdday_, 6))  %>%
    distinct(stovetype,meanday_, .keep_all = TRUE)  %>%
    mutate(day = gsub('covday_','',covday_)) %>% ungroup()
  
  coeff = .25
  
  gg_cov_indirect <-  joiners %>%
    ggplot(aes(x=day,y = meanday)) + 
    geom_errorbar(aes(ymin=meanday-sdday, ymax=meanday+sdday), width=.1,color = meanColor) +
    geom_point(color = meanColor) +
    geom_line(color = meanColor) +
    geom_point(aes(x=day,y=covday/coeff),size=3,color = COVColor) +
    labs(x="Successive days monitoring") +
    theme_minimal(base_size = 16)+
    facet_wrap(~stovetype, ncol = 1) + 
    scale_y_continuous(
      # Features of the first axis
      name = expression(paste(PM[2.5], " indirect exposure (", mu, "g ", m^-3,")")),
      
      # Add a second axis and specify its features
      sec.axis = sec_axis(~.*coeff, name="Coefficient of variation (%)")
    ) + 
    theme(
      axis.title.y = element_text(color = meanColor, size=13),
      axis.title.y.right = element_text(color = COVColor, size=13)
    ) 
  
  ggsave("Results/indirect_means_intensive.png",plot=gg_cov_indirect,width = 8, height = 6,dpi=300,device=NULL)
  
}






