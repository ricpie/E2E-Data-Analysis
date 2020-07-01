#Function for generating summary statistics and plots separate from walkthrough results and indirect exposure modeling results.


summary_results_function = function(meta_emissions,all_merged,preplacement,tsi_meta_qaqc){
  
  give.n <- function(x){return(c(y = 0, label = length(x)))}
  give.median <- function(x){return(c(y =median(x)+3, label = round(median(x),digits=2)))}
  
  kitchen_volume_plot <- ggplot(meta_emissions, aes(y=roomvolume,x=1)) +
    geom_boxplot(alpha = 0.25) +
    geom_jitter(height = 0,width = 0.2,alpha = 0.2) +
    theme_minimal() +
    ylab("m^3") + xlab("") + theme( axis.text.x = element_blank()) +
    geom_text(aes(y = max(roomvolume),x=.7 ,label = paste("Median=",round(median(roomvolume),1))),size=3) +
    geom_text(aes(y = max(roomvolume)*.95,x=.7 ,label = paste("SD=",round(sd(roomvolume),1))),size=3) +
    geom_text(aes(y = max(roomvolume)*.9,x=.7 ,label = paste("n=",length(roomvolume))),size=3) +
    ggtitle('Kitchen Volume')
  #Plot kitchen volume distributions
  
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/kitchen_volume_plot.png",plot=last_plot(),dpi=200,device=NULL)
  
  #Plot ventilation rate distributions
  
  aer_plot <- ggplot(tsi_meta_qaqc[!is.na(tsi_meta_qaqc$AERslope),], aes(y=-AERslope,x=1)) +
    geom_boxplot(alpha = 0.25) +
    geom_jitter(height = 0,width = 0.2,alpha = 0.2) +
    theme_minimal() +
    ylab("AER (changes/hour)") + xlab("")  + theme( axis.text.x = element_blank()) +
    geom_text(aes(y = max(-AERslope),x=.7 ,label = paste("Median=",round(median(-AERslope),1))),size=3) +
    geom_text(aes(y = max(-AERslope)*.95,x=.7 ,label = paste("SD=",round(sd(-AERslope),1))),size=3) +
    geom_text(aes(y = max(-AERslope)*.9,x=.7 ,label = paste("n=",length(-AERslope))),size=3) +
    ggtitle('Air Exchange Rate from TSI')
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/aer_tsi_plot.png",plot=last_plot(),dpi=200,device=NULL)
  
  #Plot event duration distributions
  meta_emissions_summarized = summarise(group_by(meta_emissions, stovetype), 
                             medeventduration = median(eventduration),
                             sdeventduration = sd(eventduration),
                             maxeventduration = max(eventduration),
                             n_eventduration = length(eventduration)) %>% dplyr::ungroup() %>%
    mutate(maxeventduration = max(maxeventduration))
  
    aer_plot <- ggplot(meta_emissions, aes(y=eventduration,x=0)) +
      facet_grid( ~ stovetype,scales = "free", space = "free", labeller = label_wrap_gen(width = 12, multi_line = TRUE)) +
      geom_boxplot(alpha = 0.25) +
      geom_jitter(height = 0,width = 0.2,alpha = 0.2) +
      theme_minimal() +
      ylab("Minutes")  +xlab("")+ theme( axis.text.x = element_blank()) +
      geom_text(data=meta_emissions_summarized,aes(y = maxeventduration, x=-.2 ,label = paste("Median=",round(medeventduration,1))),size=3) +
      geom_text(data=meta_emissions_summarized,aes(y = maxeventduration*.95, x=-.2 ,label = paste("SD=",round(sdeventduration,1))),size=3) +
      geom_text(data=meta_emissions_summarized,aes(y = maxeventduration*.9, x=-.2 ,label = paste("n=",round(n_eventduration,1))),size=3) +
      ggtitle('Cooking event duration')
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/event_duration_plot.png",plot=last_plot(),dpi=200,device=NULL)
  
  #Plot exposures
  scatter_ecm_lpgpercent <- timeseries_plot(ecm_meta_data %>% filter(qc == 'good') 
                                            ,y_var = "`PM µgm-3`", facet_var = "pm_location", x_var = 'lpg_percent',size_var = 'non_lpg_cooking') 
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/scatter_ecm_lpgpercent.png",plot=last_plot(),dpi=200,device=NULL)
  
  
  dist_ecm_lpgpercent <- timeseries_plot(ecm_meta_data %>% filter(qc == 'good') 
                                         , y_var = "`PM µgm-3`", facet_var = "pm_location", x_var = 'lpg_percent',size_var = 'non_lpg_cooking') 
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/dist_ecm_lpgpercent.png",plot=last_plot(),dpi=200,device=NULL)
  
  
  boxplot_ecm_kitchen_cook <- box_plot_facet(ecm_meta_data %>% filter(qc == 'good') 
                                             , y_var = "`PM µgm-3`", facet_var = "pm_location", x_var = "primary_stove", y_units = "µgm-3",title = "ECM PM2.5 concentration" )
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/boxplot_ecm_kitchen_cook.png",plot=last_plot(),dpi=200,device=NULL)
  
  scatter_ecm_kitchen_cook <- timeseries_plot_simple(pivot_wider(ecm_meta_data,
                                                                 names_from = pm_location,
                                                                 values_from = c(`PM µgm-3`,datetime_start,datetime_end,
                                                                                 sampling_duration,samplerate_minutes,lpg_cooking,non_lpg_cooking,lpg_percent)) %>%
                                                       rename(`Cook's PM µgm-3` = `PM µgm-3_Cook`,
                                                              `Kitchen PM µgm-3` = `PM µgm-3_Kitchen`),
                                                     y_var = "`Cook's PM µgm-3`", x_var = "`Kitchen PM µgm-3`")
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/scatter_ecm_kitchen_cook.png",plot=last_plot(),dpi=200,device=NULL)
  
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/scatter_ecm_lpgpercent.png",plot=last_plot(),dpi=200,device=NULL)
  
  
  #Plot kitchen concentration distributions
  HAP_CO_plot <- box_plot(lascar_meta  %>% as.data.frame() %>% left_join(meta_emissions,by="HHID")
                          , y_var = "eventduration", fill_var = "stovetype", x_var = "sampletype", y_units = "ppm",title = "CO concentration" )
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/HAP_CO_plot.png",plot=last_plot(),dpi=200,device=NULL)
  
  HAP_PM_plot <- box_plot(pats_meta_qaqc %>% sampletype_fix_function() %>% as.data.frame() %>% left_join(meta_emissions,by="HHID")
                          , y_var = "eventduration", fill_var = "stovetype", x_var = "sampletype", y_units = "µgm-3",title = "PATS+ PM concentration" )
  ggsave("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/HAP_PM_plot.png",plot=last_plot(),dpi=200,device=NULL)
  
  
}
