

ecm_process_metadata <- function(ecm_dot_data, output=c('raw_data', 'meta_data','preplacement'), local_tz,preplacement){
  # preplacement<-mobenzilist[[11]]
  
  #For each deployment, get the averages and start/stop times.  Check CAA code to make sure we are only considering unique values/not duplicating as there are repeats of the exposure data by stove type.
  ecm_meta_data_og <- ecm_dot_data %>%
    dplyr::group_by(pm_location,pm_hhid_numeric,HHID,mission_name,stove_type) %>%
    dplyr::summarise(datetime_start = min(datetime,na.rm=TRUE), 
                     datetime_end = max(datetime,na.rm=TRUE), 
                     sampling_duration = difftime(datetime_end,datetime_start,unit='mins'),
                     samplerate_minutes = as.numeric(median(difftime(datetime,lag(datetime,1),unit='mins'),na.rm=TRUE)),
                     `PM µgm-3` = mean(pm25_conc,na.rm=TRUE),
                     cooking_time = sum(cooking==TRUE)*samplerate_minutes) %>%
    dplyr::mutate(qc = case_when(sampling_duration > 1440*.9  ~ 'good',
                                 TRUE ~ 'bad')) %>%
    dplyr::ungroup()
  
  
  ecm_meta_data2 <- ecm_dot_data %>%
    dplyr::group_by(pm_location,pm_hhid_numeric,HHID,mission_name,stove_type) %>%
    dplyr::summarise(datetime_start = min(datetime,na.rm=TRUE), 
                     datetime_end = max(datetime,na.rm=TRUE), 
                     sampling_duration = difftime(datetime_end,datetime_start,unit='mins'),
                     samplerate_minutes = as.numeric(median(difftime(datetime,lag(datetime,1),unit='mins'),na.rm=TRUE)),
                     `PM µgm-3` = mean(pm25_conc,na.rm=TRUE),
                     cooking_time = sum(cooking==TRUE)*samplerate_minutes) %>%
    dplyr::mutate(qc = case_when(sampling_duration > 1440*.9  ~ 'good',
                                 TRUE ~ 'bad')) %>%
    dplyr::filter(qc == 'good') %>%
    dplyr::group_by(pm_location,pm_hhid_numeric,HHID, stove_type)  %>% #regroup so we can combine cooking time from multiple stoves
    dplyr::summarise(datetime_start = min(datetime_start,na.rm=TRUE), 
                     datetime_end = max(datetime_end,na.rm=TRUE), 
                     sampling_duration = max(sampling_duration),
                     samplerate_minutes = max(samplerate_minutes),
                     `PM µgm-3` = max(`PM µgm-3`) ,
                     cooking_time = sum(cooking_time),
                     # lpg_percent = cooking_time[stove_type=="lpg"]/cooking_time[stove_type!="lpg"]
    ) 
  
  ecm_meta_data <- ecm_meta_data2 %>%
    dplyr::mutate(lpg_cooking = case_when(stove_type=="lpg" ~ cooking_time,
                                          stove_type!="lpg" ~ 0)) %>%
    dplyr::mutate(non_lpg_cooking = case_when(stove_type!="lpg" ~ cooking_time,
                                           stove_type=="lpg" ~ 0)) %>%
    dplyr::group_by(pm_location,pm_hhid_numeric,HHID) %>%

    dplyr::mutate(lpg_cooking = sum(lpg_cooking),
                  non_lpg_cooking = sum(non_lpg_cooking),
                  lpg_percent = 100*lpg_cooking/(lpg_cooking+non_lpg_cooking)) %>%
                  # lpg_percent = case_when(is.infinite(lpg_percent)==TRUE ~ 1,
                                        # TRUE ~ lpg_percent)) %>%
    dplyr::mutate(primary_stove  = case_when(lpg_cooking>non_lpg_cooking ~ 'lpg',
                                             lpg_cooking<non_lpg_cooking ~ 'non-lpg'),
                  secondary_stove  = case_when(lpg_cooking>non_lpg_cooking ~ 'non-lpg',
                                               lpg_cooking<non_lpg_cooking ~ 'lpg'),
                  qc = case_when(sampling_duration > 1440*.9  ~ 'good',
                                 TRUE ~ 'bad')
                  ) %>%
    
    dplyr::arrange(pm_location,HHID) %>%
    dplyr::select(-cooking_time) %>%
    dplyr::filter(row_number()==1)
    

  return(ecm_meta_data)
}  


