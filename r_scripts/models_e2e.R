##################################################
#Function to perform modeling for E2E
##################################################


model_indirect_exposure = function(all_merged_summary,all_merged,preplacement,meta_emissions){
  
  #PM Estimates
  all_merged_summary_subset <- dplyr::group_by(all_merged_summary,HHID) %>%
    left_join(meta_emissions %>%
                mutate(HHID = HHID_full,
                       Date = as.Date(`Start time (Mobenzi Pre-placement)-- 1`)), #This doesn't work, the Dates are not always the same for the emissions... check on this.
              by = c('HHID','Date')) %>%
    filter(!is.na(meanpm25_indirect_nearest)) %>%  print() %>%
    filter(meanPM25Cook>0) %>% print() %>%
    filter(meanPM25Kitchen>0) %>% print() %>%
    # filter(mean_compliance>0.2) %>% print() %>%
    filter(countlocation_nearest>1200,countPM25Cook>1200) %>%
    # filter(!HHID %like% 'KE001-KE10') %>%
    #Filter certain data points if needed.
    # mutate(PM25Cook = case_when(HHID != 'KE509-KE06' ~ PM25Cook)) %>%
    mutate(lnPM25Cook = log(meanPM25Cook),
           lnPM25Kitchen = log(meanPM25Kitchen),
           lnPMPATSLivingRoom = log(meanPM25LivingRoom),
           lnmeanpm25_indirect_nearest = log(meanpm25_indirect_nearest),
           lnmeanpm25_indirect_nearest_threshold = log(meanpm25_indirect_nearest_threshold),
           lnmeanpm25_indirect_nearest_threshold80 = log(meanpm25_indirect_nearest_threshold80)
           # lnpm25_conc_beacon_pats_threshold = log(pm25_conc_beacon_pats_threshold),
           # lnpm25_conc_beacon_pats = log(pm25_conc_beacon_pats)
    )
  
  beacon_ecm_nearest <- ggplot(all_merged_summary_subset, aes(y = meanPM25Cook, x = meanpm25_indirect_nearest, size = mean_compliance)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') +
    geom_text(data = subset(all_merged_summary_subset, meanpm25_indirect_nearest > 600), 
              aes(y = meanPM25Cook, x = meanpm25_indirect_nearest, label = HHID),hjust = -.1) + 
    geom_text(data = subset(all_merged_summary_subset, meanpm25_indirect_nearest > 1000), 
              aes(y = meanPM25Cook, x = meanpm25_indirect_nearest, label = HHID),hjust = 1) + 
    geom_abline(slope=1, intercept=0,color = 'red')
  
  beacon_ecm_nearest_mod <- summary(lm(meanpm25_indirect_nearest ~ meanPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  lnbeacon_ecm_nearest <- ggplot(all_merged_summary_subset, aes(y = lnPM25Cook, x = lnmeanpm25_indirect_nearest, size = mean_compliance,label=HHID)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') +
    geom_abline(slope=1, intercept=0,color='red')
  
  lnbeacon_ecm_nearest_mod <- summary(lm(lnmeanpm25_indirect_nearest ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  
  beacon_ecm_thresh <- ggplot(all_merged_summary_subset, aes(y = PM25Cook, x = pm25_conc_beacon_nearestthreshold_ecm, size = mean_compliance)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # beacon_ecm_mod_thresh <- summary(lm(pm25_conc_beacon_nearestthreshold_ecm ~ PM25Cook, data = all_merged_summary_subset)) %>% print()
  lnbeacon_ecm_thresh <- ggplot(all_merged_summary_subset, aes(y = lnPM25Cook, x = lnmeanpm25_indirect_nearest_threshold, size = mean_compliance,label=HHID)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnbeacon_ecm_thresh_mod <- summary(lm(lnmeanpm25_indirect_nearest_threshold ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  lnbeacon_ecm_thresh_mod80 <- summary(lm(log(lnmeanpm25_indirect_nearest_threshold80) ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  
  # beacon_pats <- ggplot(all_merged_summary_subset, aes(y = PM25Cook, x = pm25_conc_beacon_pats, size = mean_compliance)) +
    # geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # beacon_pats_mod <- summary(lm(pm25_conc_beacon_pats ~ PM25Cook, data = all_merged_summary_subset)) %>% print()
  # lnbeacon_pats <- ggplot(all_merged_summary_subset, aes(y = lnPM25Cook, x = lnpm25_conc_beacon_pats, size = mean_compliance,label=HHID)) +
  #   geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # lnbeacon_pats_mod <- summary(lm(lnpm25_conc_beacon_pats ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  beacon_pats_thresh <- ggplot(all_merged_summary_subset, aes(y = PM25Cook, x = pm25_conc_beacon_pats_threshold, size = mean_compliance)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # beacon_pats_thresh_mod <- summary(lm(pm25_conc_beacon_pats_threshold ~ PM25Cook, data = all_merged_summary_subset)) %>% print()
  lnbeacon_pats_thresh <- ggplot(all_merged_summary_subset, aes(y = lnPM25Cook, x = lnpm25_conc_beacon_pats_threshold, size = mean_compliance,label=HHID)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # lnbeacon_pats_thresh_mod <- summary(lm(lnpm25_conc_beacon_pats_threshold ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  # lnbeacon_pats_thresh_mod80 <- summary(lm(log(pm25_conc_beacon_pats_threshold80) ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  
  pmcook_kitchen <- ggplot(all_merged_summary_subset, aes(y = PM25Cook, x = PM25Kitchen, size = mean_compliance)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # pmcook_kitchen_mod <- summary(lm(PM25Kitchen ~ PM25Cook, data = all_merged_summary_subset)) %>% print()
  lnpmcook_kitchen <- ggplot(all_merged_summary_subset, aes(y = lnPM25Cook, x = lnPM25Kitchen, size = mean_compliance,label=HHID)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnpmcook_kitchen_mod <- summary(lm(lnPM25Kitchen ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  
  lnpmecm_pats <- ggplot(all_merged_summary_subset, aes(y = lnPMPATSKitchen, x = lnPM25Kitchen, size = mean_compliance,label=HHID)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # lnpmecm_pats_mod <- summary(lm(lnPM25Kitchen ~ lnPMPATSKitchen, data = all_merged_summary_subset)) %>% print()
  
  
  lnpmKitchenCook_pats <- ggplot(all_merged_summary_subset, aes(y = lnPMPATSKitchen, x = lnPMPATSLivingRoom, size = mean_compliance,label=HHID)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # lnpmKitchenCook_pats_mod <- summary(lm(lnPMPATSLivingRoom ~ lnPMPATSKitchen, data = all_merged_summary_subset)) %>% print()
  
  #CO Estimates
  all_merged_summary_subset_co <- all_merged_summary %>% filter(!is.na(meanCO_indirect_nearest_threshold)) %>%
    filter(meanCO_ppmCook>0) %>% print() %>%
    filter(meanCO_ppmKitchen>0) %>% print() %>%
    mutate(lnCO_ppmCook = log(meanCO_ppmCook+.1),
           lnCO_ppmKitchen = log(meanCO_ppmKitchen+.1),
           lnco_estimate_beacon_nearest_threshold = log(meanCO_indirect_nearest_threshold+.1),
           lnco_estimate_beacon_nearest = log(meanCO_indirect_nearest+.1)
    )
  
  beacon_CO_nearest_threshold <- ggplot(all_merged_summary_subset_co, aes(y = meanCO_indirect_nearest_threshold, x = meanCO_ppmCook, size = mean_compliance)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # beacon_CO_nearest_mod <- summary(lm(co_estimate_beacon_nearest_threshold ~ meanCO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  lnbeacon_CO_nearest_threshold <- ggplot(all_merged_summary_subset_co, aes(y = lnco_estimate_beacon_nearest_threshold, x = lnCO_ppmCook, size = mean_compliance,label=HHID)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnbeacon_CO_nearest_mod <- summary(lm(lnco_estimate_beacon_nearest_threshold ~ lnCO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  
  beacon_CO_nearest <- ggplot(all_merged_summary_subset_co, aes(y = co_estimate_beacon_nearest, x = meanCO_ppmCook, size = mean_compliance)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # beacon_CO_mod <- summary(lm(co_estimate_beacon_nearest ~ CO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  lnbeacon_CO_nearest <- ggplot(all_merged_summary_subset_co, aes(y = lnco_estimate_beacon_nearest, x = lnCO_ppmCook, size = mean_compliance,label=HHID)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnbeacon_CO_mod <- summary(lm(lnco_estimate_beacon_nearest ~ lnCO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  
  cocook_kitchen <- ggplot(all_merged_summary_subset_co, aes(y = CO_ppmKitchen, x = meanCO_ppmCook, size = mean_compliance)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  # pmcook_kitchen_mod <- summary(lm(CO_ppmKitchen ~ CO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  lncocook_kitchen <- ggplot(all_merged_summary_subset_co, aes(y = lnCO_ppmKitchen, x = lnCO_ppmCook, size = mean_compliance,label=HHID)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnpmcook_kitchen_mod <- summary(lm(lnCO_ppmKitchen ~ lnCO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  
  
  
  # stats_by_location(all_merged_summary
}

#24h exposure, and exposure during cooking event.  Estimated using fraction of hap measures vs. personal exposure
model_indirect_ratio = function(all_merged,preplacement){
  
  
}


#24h exposure, and exposure during cooking event.  Estimated with beacon and hap data vs. personal exposure
model_indirect_beacon = function(all_merged,preplacement){
  
  
  
}




