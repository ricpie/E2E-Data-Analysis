#Function for generating summary statistics and plots separate from walkthrough results and indirect exposure modeling results.


summary_results_function = function(meta_emissions,all_merged,preplacement,tsi_meta_qaqc){
  
  give.n <- function(x){return(c(y = 0, label = length(x)))}
  give.median <- function(x){return(c(y =median(x)+3, label = round(median(x),digits=2)))}
  f <- function(x) {
    r <- quantile(x, probs = c(0.05, 0.25, 0.5, 0.75, 0.95))
    names(r) <- c("ymin", "lower", "middle", "upper", "ymax")
    r}
  
  
  #Plot event duration distributions
  event_summary = meta_emissions %>% 
    dplyr::mutate(maxmax = max(eventduration,na.rm=T)) %>%
    dplyr::group_by(stovetype) %>% 
    dplyr::summarise(means=round(mean(eventduration,na.rm = T),1),
                     sd=round(sd(eventduration,na.rm = T),1),
                     max=max(maxmax),
                     n=n())
  event_duration <- ggplot(meta_emissions, aes(y=eventduration,x=0)) +
    facet_grid( ~ stovetype,scales = "free", space = "free", labeller = label_wrap_gen(width = 12, multi_line = TRUE)) +
    geom_boxplot(alpha = 0.25) +
    stat_summary(fun.data = f,geom="boxplot")+
    geom_jitter(height = 0,width = 0.2,alpha = 0.2) +
    theme_minimal() +
    theme(text=element_text(size=16), axis.text.x = element_blank()) +
    ylab("Minutes")  +xlab("")+ 
    ggtitle('Cooking event duration') + 
    facet_grid( ~ stovetype,scales = "free", space = "free", labeller = label_wrap_gen(width = 12, multi_line = TRUE)) +
    geom_text(data=event_summary,aes(y = max,x=0,label=paste0("mean= ", means)), size=5,nudge_y =1) +
    geom_text(data=event_summary,aes(y = max*.93,x=0,label=paste0("sd= ",sd)) ,size=5,nudge_y =1) + 
    geom_text(data=event_summary,aes(y = max*.85,x=0,label=paste0("n= ", n)), size=5,nudge_y =1)
  
  
  volume_summary = meta_emissions %>% 
    dplyr::mutate(maxmax = max(`Kitchen Volume (m3)...270`,na.rm=T)) %>%
    dplyr::group_by(stovetype) %>% 
    dplyr::summarise(means=round(mean(`Kitchen Volume (m3)...270`,na.rm = T),1),
                     sd=round(sd(`Kitchen Volume (m3)...270`,na.rm = T),1),
                     max=max(maxmax),
                     n=n())
  volume_plot <- ggplot(meta_emissions, aes(y=`Kitchen Volume (m3)...270`,x=1)) +
    geom_boxplot(alpha = 0.25) +
    stat_summary(fun.data = f,geom="boxplot")+
    geom_jitter(height = 0,width = 0.2,alpha = 0.2) +
    theme_minimal() +
    theme(text=element_text(size=16), axis.text.x = element_blank()) +
    ylab(expression(M^3)) + xlab("") +
    ggtitle('Kitchen Volume') +
    facet_grid( ~ stovetype,scales = "free", space = "free", labeller = label_wrap_gen(width = 12, multi_line = TRUE)) +
    geom_text(data=volume_summary,aes(y = max,x=1,label=paste0("mean= ", means)), size=5,nudge_y =1) +
    geom_text(data=volume_summary,aes(y = max*.93,x=1,label=paste0("sd= ",sd)) ,size=5,nudge_y =1) + 
    geom_text(data=volume_summary,aes(y = max*.85,x=1,label=paste0("n= ", n)), size=5,nudge_y =1)
  
  
  aer_summary = meta_emissions %>% 
    dplyr::mutate(maxmax = max(ACH...271,na.rm=T)) %>%
    dplyr::group_by(stovetype) %>% 
    dplyr::summarise(means=round(mean(ACH...271,na.rm = T),1),
                     sd=round(sd(ACH...271,na.rm = T),1),
                     max=max(maxmax),
                     n=n())
  aer_plot <- ggplot(meta_emissions, aes(y=ACH...271,x=1)) +
    facet_grid( ~ stovetype,scales = "free", space = "free", labeller = label_wrap_gen(width = 12, multi_line = TRUE)) +
    geom_boxplot(alpha = 0.25) +
    stat_summary(fun.data = f,geom="boxplot")+
    geom_jitter(height = 0,width = 0.2,alpha = 0.2) +
    theme_minimal() +
    theme(text=element_text(size=16), axis.text.x = element_blank()) +
    ylab("ACH (changes/hour)") + xlab("")  +
    ggtitle('Air Exchange Rate') + 
    facet_grid( ~ stovetype,scales = "free", space = "free", labeller = label_wrap_gen(width = 12, multi_line = TRUE)) +
    geom_text(data=aer_summary,aes(y = max,x=1,label=paste0("mean= ", means)), size=5,nudge_y =1) +
    geom_text(data=aer_summary,aes(y = max*.93,x=1,label=paste0("sd= ",sd)) ,size=5,nudge_y =1) + 
    geom_text(data=aer_summary,aes(y = max*.85,x=1,label=paste0("n= ", n)), size=5,nudge_y =1)
  
  
  plot_name = paste0("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/ACH_duration_volume_dist.jpeg")
  
  jpeg(plot_name,width = 1000, height = 800, units = "px",quality=100)

  egg::ggarrange(aer_plot,volume_plot,event_duration, heights = c(0.3, 0.3,.3))
  
  dev.off()
  
  
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
