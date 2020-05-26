##################################################
#Function to perform modeling for E2E
##################################################


model_indirect_exposure = function(all_merged_summary,all_merged,preplacement,meta_emissions){
  
  #PM Estimates
  all_merged_summary_subset <- all_merged_summary %>% 
    filter(!is.na(pm25_conc_beacon_nearest_ecm)) %>%  print() %>%
    filter(PM25Cook>0) %>% print() %>%
    filter(PM25Kitchen>0) %>% print() %>%
    # filter(pm_compliant>0.2) %>% print() %>%
    mutate(lnPM25Cook = log(PM25Cook),
           lnPM25Kitchen = log(PM25Kitchen),
           lnpm25_conc_beacon_nearest_ecm = log(pm25_conc_beacon_nearest_ecm),
           lnpm25_conc_beacon_nearestthreshold_ecm = log(pm25_conc_beacon_nearestthreshold_ecm),
           lnpm25_conc_beacon_pats = log(pm25_conc_beacon_pats)
           )
    
  beacon_ecm_nearest <- ggplot(all_merged_summary_subset, aes(y = PM25Cook, x = pm25_conc_beacon_nearest_ecm, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  beacon_ecm_nearest_mod <- summary(lm(pm25_conc_beacon_nearest_ecm ~ PM25Cook, data = all_merged_summary_subset)) %>% print()
  lnbeacon_ecm_nearest <- ggplot(all_merged_summary_subset, aes(y = lnPM25Cook, x = lnpm25_conc_beacon_nearest_ecm, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnbeacon_ecm_nearest_mod <- summary(lm(lnpm25_conc_beacon_nearest_ecm ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  
  beacon_ecm <- ggplot(all_merged_summary_subset, aes(y = PM25Cook, x = pm25_conc_beacon_nearestthreshold_ecm, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  beacon_ecm_mod <- summary(lm(pm25_conc_beacon_nearestthreshold_ecm ~ PM25Cook, data = all_merged_summary_subset)) %>% print()
  lnbeacon_ecm <- ggplot(all_merged_summary_subset, aes(y = lnPM25Cook, x = lnpm25_conc_beacon_nearestthreshold_ecm, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnbeacon_ecm_mod <- summary(lm(lnpm25_conc_beacon_nearestthreshold_ecm ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  beacon_pats <- ggplot(all_merged_summary_subset, aes(y = PM25Cook, x = pm25_conc_beacon_pats, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  beacon_pats_mod <- summary(lm(pm25_conc_beacon_pats ~ PM25Cook, data = all_merged_summary_subset)) %>% print()
  lnbeacon_pats <- ggplot(all_merged_summary_subset, aes(y = lnPM25Cook, x = lnpm25_conc_beacon_pats, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnbeacon_pats_mod <- summary(lm(lnpm25_conc_beacon_pats ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  pmcook_kitchen <- ggplot(all_merged_summary_subset, aes(y = PM25Cook, x = PM25Kitchen, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  pmcook_kitchen_mod <- summary(lm(PM25Kitchen ~ PM25Cook, data = all_merged_summary_subset)) %>% print()
  lnpmcook_kitchen <- ggplot(all_merged_summary_subset, aes(y = lnPM25Cook, x = lnPM25Kitchen, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnpmcook_kitchen_mod <- summary(lm(lnPM25Kitchen ~ lnPM25Cook, data = all_merged_summary_subset)) %>% print()
  
  
  #CO Estimates
  all_merged_summary_subset_co <- all_merged_summary %>% filter(!is.na(co_estimate_beacon_nearest_threshold)) %>%
    # filter(CO_ppmCook>0) %>% print() %>%
    # filter(CO_ppmKitchen>0) %>% print() %>%
    mutate(lnCO_ppmCook = log(CO_ppmCook),
           lnCO_ppmKitchen = log(CO_ppmKitchen),
           lnco_estimate_beacon_nearest_threshold = log(co_estimate_beacon_nearest_threshold),
           lnco_estimate_beacon_nearest = log(co_estimate_beacon_nearest)
    )
  
  beacon_CO_nearest_threshold <- ggplot(all_merged_summary_subset_co, aes(y = co_estimate_beacon_nearest_threshold, x = CO_ppmCook, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  beacon_CO_nearest_mod <- summary(lm(co_estimate_beacon_nearest_threshold ~ CO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  lnbeacon_CO_nearest_threshold <- ggplot(all_merged_summary_subset_co, aes(y = lnco_estimate_beacon_nearest_threshold, x = lnCO_ppmCook, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnbeacon_CO_nearest_mod <- summary(lm(lnco_estimate_beacon_nearest_threshold ~ lnCO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  
  beacon_CO_nearest <- ggplot(all_merged_summary_subset_co, aes(y = co_estimate_beacon_nearest, x = CO_ppmCook, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  beacon_CO_mod <- summary(lm(co_estimate_beacon_nearest ~ CO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  lnbeacon_CO_nearest <- ggplot(all_merged_summary_subset_co, aes(y = lnco_estimate_beacon_nearest, x = lnCO_ppmCook, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnbeacon_CO_mod <- summary(lm(lnco_estimate_beacon_nearest ~ lnCO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  
  pmcook_kitchen <- ggplot(all_merged_summary_subset_co, aes(y = CO_ppmKitchen, x = CO_ppmCook, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  pmcook_kitchen_mod <- summary(lm(CO_ppmKitchen ~ CO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  lnpmcook_kitchen <- ggplot(all_merged_summary_subset_co, aes(y = lnCO_ppmKitchen, x = lnCO_ppmCook, size = pm_compliant)) +
    geom_point(alpha = 0.3) + theme_minimal() +  geom_smooth(method = "lm", formula = 'y ~ x',color = 'black') 
  lnpmcook_kitchen_mod <- summary(lm(lnCO_ppmKitchen ~ lnCO_ppmCook, data = all_merged_summary_subset_co)) %>% print()
  
  
  
  stats_by_location(all_merged_summary
}

#24h exposure, and exposure during cooking event.  Estimated using fraction of hap measures vs. personal exposure
model_indirect_ratio = function(all_merged,preplacement){
  
  
}


#24h exposure, and exposure during cooking event.  Estimated with beacon and hap data vs. personal exposure
model_indirect_beacon = function(all_merged,preplacement){
  

  
}




