###NOTES
# the primary stove recode is not correct
# manufactured == charcoal
# resolve either by tossing the stove_class variable
# or using the primary_stove variable

options(width=system("tput cols", intern=TRUE))

library(data.table)
library(ggplot2)

#helper(s)
return_cols_like <- function(dataframe, like, invert = FALSE){
  if(invert){colnames(dataframe)[!colnames(dataframe) %like% like]}else{
  colnames(dataframe)[colnames(dataframe) %like% like]}
}

##############################
###DATA IMPORT AND CLEANING###
##############################
# Import Ricardo's datasets
unops <- readRDS('~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/all_merged.rds')
#recode a stove or two
unops[, unique(pm_primary_stove)]
unops[pm_primary_stove == "LPG", pm_primary_stove := 'Cooking gas/LPG stove']
unops[pm_primary_stove == "Chipkube", pm_primary_stove := 'Manufactured solid fuel stove']

#extract stove types for later merging
stove_types <- unops[, list(unique_primary_stove = unique(pm_primary_stove)), by = 'HHID,HHIDnumeric']

#SES indices from RP
ses <- as.data.table(readRDS('~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/predicted_ses.rds'))
ses[, id := NULL]

#Emissions metadata
meta_emissions <- as.data.table(readRDS('~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/meta_emissions.RDS'))
meta_emissions[, date:= as.Date(datetime_sample_start, tz = "Africa/Nairobi")]
meta_emissions_cols <- c("HHID_full", 'date', return_cols_like(meta_emissions, "Window|window|eaves|Eaves|door|Door|Kitchen Volume"))
meta_emissions_subset <- meta_emissions[, meta_emissions_cols, with = F]
meta_emissions_subset[, `Height of open window 4` := NULL]
meta_emissions_subset[, `Width of open window 4` := NULL]
setnames(meta_emissions_subset, c('HHID_full', 'date', 'window1_h', 'window1_w', 'window2_h', 'window2_w', 'window3_h', 'window3_w', 'door1_h', 'door1_w', 'door2_h','door2_w', 'walls_w_eaves_n', 'volume', 'volume_2'))
meta_emissions_subset[, volume_2 := NULL]
#calculate window and door areas
meta_emissions_subset[, window1 := window1_h * window1_w]
meta_emissions_subset[, window2 := window2_h * window2_w]
meta_emissions_subset[, window3 := window3_h * window3_w]
meta_emissions_subset[, door1 := door1_h * door1_w]
meta_emissions_subset[, door2 := door2_h * door2_w]
#create a total windows and door area variable
meta_emissions_subset[, door_win_m2 := sum(c(window1, window2, window3, door1, door2), na.rm=T), by='HHID_full']
#subset to required variables
meta_emissions_subset <- meta_emissions_subset[, c('HHID_full', 'date', 'walls_w_eaves_n', 'volume', 'door_win_m2')]
setnames(meta_emissions_subset, 1, 'HHID')
setnames(meta_emissions_subset, 'date', 'Date')
#deal with duplicated entries with different dates
meta_emissions_subset <- unique(meta_emissions_subset[, -c('Date')])

#summarized data from RP
unops_summary <- as.data.table(readRDS('~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/all_merged_summary.rds'))
#extract intensive data
intensive <- unops_summary[, c("HHID", return_cols_like(unops_summary, 'Intensive|intensive')), with = F]
intensive_hhids <- unops_summary[!is.na(CookingmeanPM25Kitchen1m) | !is.na(CookingmeanPM25Kitchen1m), HHID]
intensive <- intensive[HHID %in% intensive_hhids]
#drop intensive columns
all_merged_summary <- unops_summary[, return_cols_like(unops_summary, 'Intensive|intensive', invert = TRUE), with = F]

#filter per ricardo's suggestions
all_merged_summary <- all_merged_summary[meanPM25Cook > 0 & meanPM25Kitchen > 0 & !is.na(meanpm25_indirect_nearest) & countlocation_nearest>1200 & countPM25Cook>1200]
all_merged_summary <- merge(all_merged_summary, meta_emissions_subset, all.x = TRUE, by = c('HHID'))

# Merge summary with SES - resolve some duplicate issues
# merge(all_merged_summary, ses, by = "HHID")
# take mean score for duplicated households. Don't know what else to do at this point
ses[HHID %in% all_merged_summary[HHID %in% ses[duplicated(HHID), HHID], HHID], score:=mean(score), by='HHID']
# ses[HHID %in% all_merged_summary[HHID %in% ses[duplicated(HHID), HHID], HHID]]
# unique(ses)
all_merged_summary <- merge(all_merged_summary, unique(ses), by = "HHID")

#AERs from MR/RP
aers <- as.data.table(readxl::read_excel('~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/E2E_FinalAERs.xlsx'))
setnames(aers, c('Date', 'HHID', 'aer', 'method'))
aer_dup_hhids <- aers[duplicated(HHID), HHID]
aers_ok <- aers[!(HHID %in% aer_dup_hhids)]
aers_dup <- aers[HHID %in% aer_dup_hhids]
#take mean AER for duplicated HHIDs
aers_dup[, aer:=mean(aer, na.rm=T), by = HHID]
aers <- rbind(aers_dup, aers_ok)
aers[, Date := NULL]
aers[, method := NULL]
#merge summary with aer
all_merged_summary <- merge(all_merged_summary, unique(aers), by = c('HHID'), all.x = TRUE)

#add in stove types
all_merged_summary <- merge(all_merged_summary, stove_types, by = 'HHID')

##############################
##########MODEL PREP##########
##############################
#purge unnecessary columns for LMs
unops_ap <- all_merged_summary[, -c('Start_datetime', 'End_datetime', 'HHIDnumeric', 'quantile', 'emission_startstop','CookingmeanPM25Cook','CookingmeanPM25Kitchen','CookingmeanPM25Kitchen1m','CookingmeanPM25Kitchen2m','CookingmeanPM25LivingRoom','CookingmeanPM25Ambient','CookingmeanCO_ppmCook','CookingmeanCO_ppmKitchen','CookingmeanCO_ppmKitchen1m','CookingmeanCO_ppmKitchen2m','CookingmeanCO_ppmLivingRoom','CookingmeanCO_ppmAmbient','Cookingsum_TraditionalManufactured_minutes','Cookingsum_charcoal_jiko_minutes','Cookingsum_traditional_non_manufactured','Cookingsum_lpg','Cookingmeanpm25_indirect_nearest','Cookingmeanpm25_indirect_nearest_threshold','CookingmeanCO_indirect_nearest','CookingmeanCO_indirect_nearest_threshold', 'countPM25Kitchen', 'countPM25Cook', 'countPATS_LivingRoom', 'countlocation_nearest', 'countlocation_nearestthreshold', 'meanCO_ppmEmissions', 'meanCO2_ppmEmissions'
	)]

# median interpolation for missing values
unops_ap[is.na(tsiCO2ppm), tsiCO2ppm := unops_ap[, median(tsiCO2ppm, na.rm=T)]]
unops_ap[is.na(meanPM25LivingRoom), meanPM25LivingRoom := unops_ap[, median(meanPM25LivingRoom, na.rm=T)]]
unops_ap[is.na(meanPM25Ambient), meanPM25Ambient := unops_ap[, median(meanPM25Ambient, na.rm=T)]]
unops_ap[is.na(meanCO_ppmCook), meanCO_ppmCook := unops_ap[, median(meanCO_ppmCook, na.rm=T)]]
unops_ap[is.na(meanCO_ppmKitchen), meanCO_ppmKitchen := unops_ap[, median(meanCO_ppmKitchen, na.rm=T)]]
unops_ap[is.na(meanCO_ppmLivingRoom), meanCO_ppmLivingRoom := unops_ap[, median(meanCO_ppmLivingRoom, na.rm=T)]]
unops_ap[is.na(meanCO_ppmAmbient), meanCO_ppmAmbient := unops_ap[, median(meanCO_ppmAmbient, na.rm=T)]]
unops_ap[is.na(aer), aer := unops_ap[, median(aer, na.rm=T)]]

#re-type
unops_ap[, walls_w_eaves_n := as.numeric(as.character(walls_w_eaves_n))]

#log variables to prevent later headaches
unops_ap[, log_meanPM25Cook := log(meanPM25Cook)]
unops_ap[, log_meanPM25Kitchen := log(meanPM25Kitchen)]
unops_ap[, log_meanCO_ppmCook := log(meanCO_ppmCook)]
unops_ap[, log_meanCO_ppmKitchen := log(meanCO_ppmKitchen)]
unops_ap[, wday:=lubridate::wday(Date, label = TRUE)]
unops_ap[, Date := NULL]

#shorter names
setnames(unops_ap, c('hhid', 'cook_pm', 'co2', 'compliance', 'kit_pm', 'lr_pm', 'amb_pm', 'cook_co', 'kit_co', 'lr_co', 'amb_co', 'trad_man_mins', 'jiko_mins', 'trad_non_man_mins', 'bms_mins', 'bcn_pm_near', 'bcn_pm_thres', 'bcn_pm_thres80', 'bcn_co_near', 'bcn_co_thres', 'walls_with_eaves', 'kit_vol', 'door_win_area', 'ses', 'aer', 'primary_stove', 'cook_pm_log', 'kit_pm_log', 'cook_co_log', 'kit_co_log', 'day_of_week'))

#additional variables
unops_ap[primary_stove %like% 'solid fuel', stove_class := 'traditional']
unops_ap[is.na(stove_class), stove_class := 'lpg']
unops_ap[, stove_class := as.factor(stove_class)]
unops_ap[, trad_mins := trad_non_man_mins]
unops_ap[, all_stv_mins := trad_non_man_mins + trad_man_mins + bms_mins + jiko_mins]
unops_ap[, primary_stove := as.factor(primary_stove)]

#summary tables (pre-interpolation: skip lines 98-106 above)
unops_long <- melt.data.table(unops_ap, id.var = c('hhid', 'primary_stove', 'day_of_week', 'stove_class'))
unops_long[, unique(variable)]
unops_long <- unops_long[!variable %like% "_log"]

unops_summary_by_stove <- unops_long[,
	list(
		mean = round(mean(value, na.rm = T),2),
		sd = round(sd(value, na.rm = T),2),
		min = round(min(value, na.rm = T),2),
		q25 = round(quantile(value, 0.25, na.rm = T),2),
		median = round(median(value, na.rm=T),2),
		q75 = round(quantile(value, 0.75, na.rm = T),2),
		max = round(max(value, 0.75, na.rm = T,2)),
		n = length(value[!is.na(value)])
		),
	by = 'variable,stove_class'
]

unops_summary_overall <- unops_long[,
	list(
		mean = round(mean(value, na.rm = T),2),
		sd = round(sd(value, na.rm = T),2),
		min = round(min(value, na.rm = T),2),
		q25 = round(quantile(value, 0.25, na.rm = T),2),
		median = round(median(value, na.rm=T),2),
		q75 = round(quantile(value, 0.75, na.rm = T),2),
		max = round(max(value, 0.75, na.rm = T,2)),
		n = length(value[!is.na(value)])
		),
	by = 'variable'
]

# writexl::write_xlsx(list(overall = unops_summary_overall, by_stove_class = unops_summary_by_stove), '~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/model_descriptive_summary_tables.xlsx')

# unops_summary[order(stove_class,variable)]



##############################
##########MODEL RUNS##########
##############################
library(leaps) #model selection
library(performance) #for pretty plots and some rapid modeling diagnostics
library(olsrr) #part and partial correlations

#generate matrix for subsequent regressions
predict_lm_subsets <- function(object, newdata, id ,...) {
	form <- as.formula(object$call[[2]]) 
	mat <- model.matrix(form, newdata)
	coefi <- coef(object, id = id)
	xvars <- names(coefi)
	mat[, xvars] %*% coefi
}

set.seed(06152020)

unops_ap[, kef := cook_pm/kit_pm]
unops_ap[, kef_predicted := 0.742]
unops_ap[, kef_exposure_predicted := kit_pm * 0.742]

unops_lpg <- unops_ap[stove_class == 'lpg']
unops_bms <- unops_ap[primary_stove %like% 'Manufactured|manufactured']
# unops_chrcl <- unops_ap[primary_stove %like% 'Manufactured']


###########################
###### FULL DATASET #######
###########################
# k-fold CV parameters
k <- 10
n_vars <- 13
folds <- sample(1:k, nrow(unops_ap), replace = TRUE)
cv_errors <- matrix(NA, k, n_vars, dimnames = list(NULL, paste(1:n_vars)))

for(j in 1:k){
	best_subset <-  regsubsets(cook_pm_log ~ primary_stove + all_stv_mins + kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + kit_vol + door_win_area + ses + aer + bcn_pm_near, data = unops_ap[folds != j, ], nvmax = n_vars)

	for(i in 1:n_vars){
  		pred_x <- exp(predict_lm_subsets(best_subset, unops_ap[folds == j, ], id = i))
    	cv_errors[j, i] <- mean((unops_ap$cook_pm[folds == j] - pred_x)^2)
	}
}
# pred_x
# cv_errors

mean_cv_errors <- as.data.table(colMeans(cv_errors, na.rm=TRUE))
mean_cv_errors[, covariates := 1:nrow(mean_cv_errors)]
qplot(covariates, V1, data=mean_cv_errors[covariates > 3]) + geom_line() + geom_point()

final_subset_lm <-  regsubsets(cook_pm_log ~ primary_stove + all_stv_mins + kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + kit_vol + door_win_area + ses + aer + bcn_pm_near, data = unops_ap, nvmax = n_vars)
coef(final_subset_lm, 7)

unops_ap[, all_beacon := exp(predict_lm_subsets(final_subset_lm, unops_ap, id = 7))]

ggplot(aes(cook_pm, all_beacon), data=unops_ap) + 
	geom_point(color = 'coral') + 
	theme_bw() + 
	labs(
		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1) + 
	scale_x_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
	scale_y_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
	geom_abline(linetype = 'dashed') 

Metrics::rmse(unops_ap$cook_pm, unops_ap$all_beacon)
all_beacon <- lm(cook_pm_log ~  primary_stove + cook_co + kit_co + lr_co + kit_vol + bcn_pm_near, data = unops_ap)

###########################
########### KEF ###########
###########################
ggplot(aes(kef), data = unops_ap) + 
	geom_density(aes(color = stove_class, fill = stove_class), alpha = 0.1) + 
	geom_rug(aes(color = stove_class))+
	theme_bw() + 
	theme(
		strip.background = element_blank(), 
		strip.text = element_text(face = 'bold'), 
		panel.border = element_blank()
	) +
	labs(x = 'Personal Exposure / Kitchen Concentration') +
	scale_x_log10()

wilcox.test(unops_ap[stove_class == "lpg", kef], unops_ap[stove_class!='lpg', kef])
t.test(unops_ap[stove_class == "lpg", log(kef)], unops_ap[stove_class!='lpg', log(kef)])

with(unops_ap, Metrics::rmse(cook_pm, kef_exposure_predicted))
with(unops_ap[stove_class == 'lpg'], Metrics::rmse(cook_pm, kef_exposure_predicted))
with(unops_ap[stove_class != 'lpg'], Metrics::rmse(cook_pm, kef_exposure_predicted))

#exclude one extreme outlier
ggplot(aes(cook_pm, kef_exposure_predicted), data=unops_ap[kef_exposure_predicted<2000 & stove_class == 'lpg']) + 
	geom_point(color = 'coral') + 
	theme_bw() + 
	labs(
		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1, fullrange = TRUE) + 
	geom_abline(linetype = 'dashed') + 
	theme(
		strip.background = element_blank(), 
		strip.text = element_text(face = 'bold'), 
		panel.border = element_blank()
	) +
	scale_x_continuous(limits = c(0, 300), breaks = seq(0,300,100)) + 
	scale_y_continuous(limits = c(0, 300), breaks = seq(0,300,100)) 

ggplot(aes(cook_pm, kef_exposure_predicted), data=unops_ap[kef_exposure_predicted<2000 & stove_class != 'lpg']) + 
	geom_point(color = 'coral') + 
	theme_bw() + 
	labs(
		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1, fullrange = TRUE) + 
	geom_abline(linetype = 'dashed') + 
	theme(
		strip.background = element_blank(), 
		strip.text = element_text(face = 'bold'), 
		panel.border = element_blank()
	) +
	scale_x_continuous(limits = c(0, 1250), breaks = seq(0,1250,125)) + 
	scale_y_continuous(limits = c(0, 1250), breaks = seq(0,1250,125)) 

###########################
########### LMS ###########
###########################
unops_ap[, return_cols_like(unops_ap, "lmf_[0-9]{1,}") := NULL]

# primary stove only
# kitchen_pm only
# bcn_pm_thres80
# kef
# primary stove + ses

# lmf_1 <- lm(cook_pm_log ~ primary_stove, data = unops_ap)
# unops_ap[, lmf_1:=exp(predict(lmf_1))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_1))

#Summary table model 2
lmf_2 <- lm(cook_pm_log ~ kit_pm, data = unops_ap)
unops_ap[, lmf_2:=exp(predict(lmf_2))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_2))

# lmf_3 <- lm(cook_pm_log ~ bcn_pm_near, data = unops_ap)
# unops_ap[, lmf_3:=exp(predict(lmf_3))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_3))

# lmf_4 <- lm(cook_pm_log ~ all_stv_mins, data = unops_ap)
# unops_ap[, lmf_4:=exp(predict(lmf_4))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_4))

# lmf_5 <- lm(cook_pm_log ~ kef, data = unops_ap)
# unops_ap[, lmf_5:=exp(predict(lmf_5))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_5))

#Summary Table Model 1
lmf_1 <- lm(cook_pm_log ~ primary_stove + ses + kit_vol, data = unops_ap)
unops_ap[, lmf_1:=exp(predict(lmf_1))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_1))

# lmf_7 <- lm(cook_pm_log ~ primary_stove + trad_mins, data = unops_ap)
# unops_ap[, lmf_7:=exp(predict(lmf_7))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_7))

# lmf_8 <- lm(cook_pm_log ~ primary_stove + trad_mins + ses, data = unops_ap)
# unops_ap[, lmf_8:=exp(predict(lmf_8))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_8))

# lmf_9 <- lm(cook_pm_log ~ kit_pm + primary_stove, data = unops_ap)
# unops_ap[, lmf_9:=exp(predict(lmf_9))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_9))

#summary table model 5
lmf_5 <- lm(cook_pm_log ~ kit_pm + primary_stove + ses + kit_vol, data = unops_ap)
unops_ap[, lmf_5:=exp(predict(lmf_5))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_5))

#summary table model 3
lmf_3 <- lm(cook_pm_log ~ kit_pm + kit_co, data = unops_ap)
unops_ap[, lmf_3:=exp(predict(lmf_3))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_3))

# lmf_12 <- lm(cook_pm_log ~ kit_pm + kit_co + primary_stove, data = unops_ap)
# unops_ap[, lmf_12:=exp(predict(lmf_12))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_12))

#summary table model 6
lmf_6 <- lm(cook_pm_log ~ kit_pm + kit_co + primary_stove + ses + kit_vol, data = unops_ap)
unops_ap[, lmf_6:=exp(predict(lmf_6))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_6))

# lmf_14 <- lm(cook_pm_log ~ primary_stove + kit_pm + cook_co + kit_co + lr_co + kit_vol, data = unops_ap)
# unops_ap[, lmf_14:=exp(predict(lmf_14))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_14))

# lmf_15 <- lm(cook_pm_log ~ bcn_pm_near + primary_stove, data = unops_ap)
# unops_ap[, lmf_15:=exp(predict(lmf_15))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_15))

#summary table model 4
lmf_4 <- lm(cook_pm_log ~ bcn_pm_near + primary_stove + ses + kit_vol, data = unops_ap)
unops_ap[, lmf_4:=exp(predict(lmf_4))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_4))

# lmf_17 <- lm(cook_pm_log ~ kit_pm + lr_pm + amb_pm + primary_stove + ses, data = unops_ap)
# unops_ap[, lmf_17:=exp(predict(lmf_17))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_17))

# lmf_18 <- lm(cook_pm_log ~ cook_co + kit_co, data = unops_ap)
# unops_ap[, lmf_18:=exp(predict(lmf_18))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_18))

model_overview <- function(model, dt){
	form <- formula(get(model))
	adj_r2 <- round(summary(get(model))$adj.r.squared, 2)
	datatable <- get(dt)
	pred <- paste(dt, model, sep="$")
	rmse <- round(Metrics::rmse(datatable$cook_pm, unops_ap[, get(model)]), 1)
	
	data.table(
		model = model, 
		adj_r2 = adj_r2, 
		rmse = rmse, 
		pred_mean = mean(unops_ap[, get(model)]),
		pred_sd = sd(unops_ap[, get(model)]),
		t_test_p = t.test(datatable$cook_pm, unops_ap[, get(model)])$p.value
	)
}

overview_of_lm_all <- do.call('rbind', lapply(c(paste('lmf_', 1:6, sep = ""), 'all_beacon'), model_overview, 'unops_ap'))

overview_of_lm_all

data.table(names=names(coef(all_beacon)), coef=coef(all_beacon))

# as.data.table(compare_performance(all_beacon, lmf_1, lmf_2, lmf_3, lmf_4, lmf_5, lmf_6, rank = TRUE))
	#lmf_7, lmf_8, lmf_9, lmf_10, lmf_11, lmf_12, lmf_13, lmf_14, lmf_15, lmf_16, lmf_17, lmf_18, rank = TRUE))

all_long <- melt.data.table(unops_ap[, c('cook_pm', 'kef_exposure_predicted', 'all_beacon', return_cols_like(unops_ap, 'lmf_')), with = F], id.var = 'cook_pm')

ggplot(aes(cook_pm, value), data = all_long[value<1250]) + theme_bw() + facet_wrap( ~ variable, scales = 'free_y') + geom_smooth(method = 'lm') + geom_point()
ols_correlations(lmf_1)
ols_correlations(all_beacon)

## Partial is the correlation between the variable and COP after removing the effect of other variables from both the variable and COP; semi-partial is the correlation after removing the effect of other variables from COP.


###########################
#### STRATIFIED MODELS ####
###########################
########### LPG ###########
###########################
######## NO BEACON ########
###########################
# k-fold CV parameters
k <- 10
n_vars <- 10
folds <- sample(1:k, nrow(unops_lpg), replace = TRUE)
cv_errors <- matrix(NA, k, n_vars, dimnames = list(NULL, paste(1:n_vars)))

for(j in 1:k){
	best_subset <-  regsubsets(cook_pm_log ~  kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + kit_vol + door_win_area + ses + aer, data = unops_lpg[folds != j, ], nvmax = n_vars)

	for(i in 1:n_vars){
  		pred_x <- exp(predict_lm_subsets(best_subset, unops_lpg[folds == j, ], id = i))
    	cv_errors[j, i] <- mean((unops_lpg$cook_pm[folds == j] - pred_x)^2)
	}
}
pred_x
cv_errors

mean_cv_errors <- colMeans(cv_errors, na.rm=TRUE)
plot(mean_cv_errors, type = "b")

final_subset_lm <-  regsubsets(cook_pm_log ~ kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + kit_vol + door_win_area + ses + aer, data = unops_lpg, nvmax = n_vars)
coef(final_subset_lm, 4)

unops_lpg[, lpg_no_beacon := exp(predict_lm_subsets(final_subset_lm, unops_lpg, id = 4))]

ggplot(aes(cook_pm, lpg_no_beacon), data=unops_lpg) + 
	geom_point(color = 'coral') + 
	theme_bw() + 
	labs(
		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
	scale_x_continuous(limits = c(0, 150), breaks = seq(0,150,50)) + 
	scale_y_continuous(limits = c(0, 150), breaks = seq(0,150,50)) + 
	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1) + 
	geom_abline(linetype = 'dashed') + 
	ggtitle(expression("Relationship between predicted and measured personal exposures to PM"[2.5]))

Metrics::rmse(unops_lpg$cook_pm, unops_lpg$lpg_no_beacon)
lpg_no_beacon <- lm(cook_pm_log ~ lr_pm + cook_co + kit_co + ses, data = unops_lpg)

###########################
########### LPG ###########
###########################
######### BEACON ##########
###########################
# k-fold CV parameters
k <- 10
n_vars <- 11
folds <- sample(1:k, nrow(unops_lpg), replace = TRUE)
cv_errors <- matrix(NA, k, n_vars, dimnames = list(NULL, paste(1:n_vars)))

for(j in 1:k){
	best_subset <-  regsubsets(cook_pm_log ~  kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + kit_vol + door_win_area + ses + aer + bcn_pm_near, data = unops_lpg[folds != j, ], nvmax = n_vars)

	for(i in 1:n_vars){
  		pred_x <- exp(predict_lm_subsets(best_subset, unops_lpg[folds == j, ], id = i))
    	cv_errors[j, i] <- mean((unops_lpg$cook_pm[folds == j] - pred_x)^2)
	}
}
# pred_x
# cv_errors

mean_cv_errors <- colMeans(cv_errors, na.rm=TRUE)
plot(sqrt(mean_cv_errors), type = "b")

final_subset_lm <-  regsubsets(cook_pm_log ~ kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + kit_vol + door_win_area + ses + aer + bcn_pm_near, data = unops_lpg, nvmax = n_vars)
coef(final_subset_lm, 6)

unops_lpg[, lpg_beacon := exp(predict_lm_subsets(final_subset_lm, unops_lpg, id = 6))]

ggplot(aes(cook_pm, lpg_beacon), data=unops_lpg) + 
	geom_point(color = 'coral') + 
	theme_bw() + 
	labs(
		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
	scale_x_continuous(limits = c(0, 150), breaks = seq(0,150,50)) + 
	scale_y_continuous(limits = c(0, 150), breaks = seq(0,150,50)) + 
	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1) + 
	geom_abline(linetype = 'dashed')
Metrics::rmse(unops_lpg$cook_pm, unops_lpg$lpg_beacon)
lpg_beacon <- lm(cook_pm_log ~ lr_pm + cook_co + kit_co + lr_co + ses + bcn_pm_near, data = unops_lpg)

###########################
########### LMS ###########
###########################
unops_lpg[, return_cols_like(unops_lpg, "m[0-9]{1,}") := NULL]

# m1 <- lm(cook_pm_log ~ primary_stove, data = unops_ap)
# unops_ap[, m1:=exp(predict(m1))]
# with(unops_ap, Metrics::rmse(cook_pm, m1))

lpg_2 <- lm(cook_pm_log ~ kit_pm, data = unops_lpg)
unops_lpg[, lpg_2:=exp(predict(lpg_2))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_2))

# lpg_3 <- lm(cook_pm_log ~ bcn_pm_near, data = unops_lpg)
# unops_lpg[, lpg_3:=exp(predict(lpg_3))]
# with(unops_lpg, Metrics::rmse(cook_pm, lpg_3))

# lpg_4 <- lm(cook_pm_log ~ all_stv_mins, data = unops_lpg)
# unops_lpg[, lpg_4:=exp(predict(lpg_4))]
# with(unops_lpg, Metrics::rmse(cook_pm, lpg_4))

# lpg_5 <- lm(cook_pm_log ~ kef, data = unops_lpg)
# unops_lpg[, lpg_5:=exp(predict(lpg_5))]
# with(unops_lpg, Metrics::rmse(cook_pm, lpg_5))

lpg_1 <- lm(cook_pm_log ~ ses + kit_vol, data = as.data.frame(unops_lpg))
unops_lpg[, lpg_1:=exp(predict(lpg_1))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_1))

# lpg_7 <- lm(cook_pm_log ~ trad_mins, data = unops_lpg)
# unops_lpg[, lpg_7:=exp(predict(lpg_7))]
# with(unops_lpg, Metrics::rmse(cook_pm, lpg_7))

# lpg_8 <- lm(cook_pm_log ~ trad_mins + ses, data = unops_lpg)
# unops_lpg[, lpg_8:=exp(predict(lpg_8))]
# with(unops_lpg, Metrics::rmse(cook_pm, lpg_8))

# m9 <- lm(cook_pm_log ~ kit_pm + primary_stove, data = unops_lpg)
# unops_lpg[, m9:=exp(predict(m9))]
# with(unops_lpg, Metrics::rmse(cook_pm, m9))

lpg_5 <- lm(cook_pm_log ~ kit_pm + ses + kit_vol, data = unops_lpg)
unops_lpg[, lpg_5:=exp(predict(lpg_5))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_5))

lpg_3 <- lm(cook_pm_log ~ kit_pm + kit_co, data = unops_lpg)
unops_lpg[, lpg_3:=exp(predict(lpg_3))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_3))

lpg_6 <- lm(cook_pm_log ~ kit_pm + kit_co + ses + kit_vol, data = unops_lpg)
unops_lpg[, lpg_6:=exp(predict(lpg_6))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_6))

# lpg_14 <- lm(cook_pm_log ~ kit_pm + cook_co + kit_co + lr_co + kit_vol, data = unops_lpg)
# unops_lpg[, lpg_14:=exp(predict(lpg_14))]
# with(unops_lpg, Metrics::rmse(cook_pm, lpg_14))

# lpg_15 <- lm(cook_pm_log ~ bcn_pm_near, data = unops_lpg)
# unops_lpg[, lpg_15:=exp(predict(lpg_15))]
# with(unops_lpg, Metrics::rmse(cook_pm, lpg_15))

lpg_4 <- lm(cook_pm_log ~ bcn_pm_near + ses + kit_vol, data = unops_lpg)
unops_lpg[, lpg_4:=exp(predict(lpg_4))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_4))

# lpg_17 <- lm(cook_pm_log ~ kit_pm + lr_pm + amb_pm + ses, data = unops_lpg)
# unops_lpg[, lpg_17:=exp(predict(lpg_17))]
# with(unops_lpg, Metrics::rmse(cook_pm, lpg_17))

model_overview <- function(model, dt){
	form <- formula(get(model))
	adj_r2 <- round(summary(get(model))$adj.r.squared, 2)
	datatable <- get(dt)
	pred <- paste(dt, model, sep="$")
	rmse <- round(Metrics::rmse(datatable$cook_pm, datatable[, get(model)]), 1)
	
	data.table(
		model = model, 
		adj_r2 = adj_r2, 
		rmse = rmse, 
		pred_mean = mean(datatable[, get(model)]),
		pred_sd = sd(datatable[, get(model)]),
		t_test_p = t.test(datatable$cook_pm, datatable[, get(model)])$p.value
	)
}

overview_of_lm_lpg <- do.call('rbind', lapply(c(paste('lpg_', c(1:6), sep = ""), 'lpg_beacon', 'lpg_no_beacon'), model_overview, 'unops_lpg'))

overview_of_lm_lpg

# as.data.table(compare_performance(lpg_beacon, lpg_no_beacon, lpg_2, lpg_3, lpg_4, lpg_5, lpg_6, lpg_7, lpg_8, lpg_10, lpg_11, lpg_13, lpg_14, lpg_15, lpg_16, lpg_17, rank = TRUE))

all_lpg_long <- melt.data.table(unops_lpg[, c('kef_exposure_predicted', 'cook_pm', 'lpg_beacon', 'lpg_no_beacon', return_cols_like(unops_lpg, 'lpg_')), with = F], id.var = 'cook_pm')

ggplot(aes(cook_pm, value), data = all_lpg_long) + theme_bw() + facet_wrap( ~ variable, scales = 'free_y') + geom_smooth(method = 'lm') + geom_point()

ols_correlations(lm(cook_pm_log ~ ses + kit_vol, data = as.data.frame(unops_lpg)))
ols_correlations(lpg_beacon)


###########################
########### BMS ###########
###########################
######## NO BEACON ########
###########################
unops_bms[, return_cols_like(unops_bms, "lmf_[0-9]{1,}") := NULL]
unops_bms[, return_cols_like(unops_bms, "all_beacon") := NULL]

# k-fold CV parameters
k <- 10
n_vars <- 11
folds <- sample(1:k, nrow(unops_bms), replace = TRUE)
cv_errors <- matrix(NA, k, n_vars, dimnames = list(NULL, paste(1:n_vars)))

for(j in 1:k){
	best_subset <-  regsubsets(cook_pm_log ~  primary_stove + kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + trad_mins + kit_vol + door_win_area + ses + aer, data = unops_bms[folds != j, ], nvmax = n_vars)

	for(i in 1:n_vars){
  		pred_x <- exp(predict_lm_subsets(best_subset, unops_bms[folds == j, ], id = i))
    	cv_errors[j, i] <- mean((unops_bms$cook_pm[folds == j] - pred_x)^2)
	}
}
cv_errors
mean_cv_errors <- colMeans(cv_errors, na.rm=TRUE)
plot(mean_cv_errors, type = "b")

final_subset_lm <-  regsubsets(cook_pm_log ~ primary_stove + kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + trad_mins + kit_vol + door_win_area + ses + aer, data = unops_bms, nvmax = n_vars)
coef(final_subset_lm, 7)

unops_bms[, biomass_no_beacon := exp(predict_lm_subsets(final_subset_lm, unops_bms, id = 6))]

ggplot(aes(cook_pm, biomass_no_beacon), data=unops_bms) + 
	geom_point(color = 'coral') + 
	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1, color = 'deepskyblue4', fullrange = F) + 
	theme_bw() + 
	labs(
		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
	scale_x_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
	scale_y_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
	geom_abline(linetype = 'dashed')

Metrics::rmse(unops_bms$cook_pm, unops_bms$biomass_no_beacon)
biomass_no_beacon <- lm(cook_pm_log ~ primary_stove + lr_pm + kit_co + lr_co + kit_vol + door_win_area, data = unops_bms)


###########################
########### BMS ###########
###########################
######### BEACON ##########
###########################
# k-fold CV parameters
k <- 10
n_vars <- 12
folds <- sample(1:k, nrow(unops_bms), replace = TRUE)
cv_errors <- matrix(NA, k, n_vars, dimnames = list(NULL, paste(1:n_vars)))

for(j in 1:k){
	best_subset <-  regsubsets(cook_pm_log ~ kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + trad_mins + kit_vol + door_win_area + ses + aer + bcn_pm_near, data = unops_bms[folds != j, ], nvmax = n_vars)

	for(i in 1:n_vars){
  		pred_x <- exp(predict_lm_subsets(best_subset, unops_bms[folds == j, ], id = i))
    	cv_errors[j, i] <- mean((unops_bms$cook_pm[folds == j] - pred_x)^2)
	}
}
cv_errors
mean_cv_errors <- colMeans(cv_errors, na.rm=TRUE)
plot(mean_cv_errors, type = "b")

final_subset_lm <-  regsubsets(cook_pm_log ~ kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + trad_mins + kit_vol + door_win_area + ses + aer + bcn_pm_near, data = unops_bms, nvmax = n_vars)
coef(final_subset_lm, 6)

unops_bms[, biomass_beacon := exp(predict_lm_subsets(final_subset_lm, unops_bms, id = 6))]

ggplot(aes(cook_pm, biomass_beacon), data=unops_bms) + 
	geom_point(color = 'coral') + 
	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1, color = 'deepskyblue4', fullrange = F) + 
	theme_bw() + 
	labs(
		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
	scale_x_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
	scale_y_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
	geom_abline(linetype = 'dashed')

Metrics::rmse(unops_bms$cook_pm, unops_bms$biomass_beacon)
biomass_beacon <- lm(cook_pm_log ~ lr_pm + cook_co + kit_co + lr_co + kit_vol + bcn_pm_near, data = unops_bms)

###########################
########### LMS ###########
###########################
unops_bms[, return_cols_like(unops_bms, "bms_") := NULL]

# m1 <- lm(cook_pm_log ~ primary_stove, data = unops_ap)
# unops_ap[, m1:=exp(predict(m1))]
# with(unops_ap, Metrics::rmse(cook_pm, m1))

bms_2 <- lm(cook_pm_log ~ kit_pm, data = unops_bms)
unops_bms[, bms_2:=exp(predict(bms_2))]
with(unops_bms, Metrics::rmse(cook_pm, bms_2))

# bms_3 <- lm(cook_pm_log ~ bcn_pm_near, data = unops_bms)
# unops_bms[, bms_3:=exp(predict(bms_3))]
# with(unops_bms, Metrics::rmse(cook_pm, bms_3))

# bms_4 <- lm(cook_pm_log ~ all_stv_mins, data = unops_bms)
# unops_bms[, bms_4:=exp(predict(bms_4))]
# with(unops_bms, Metrics::rmse(cook_pm, bms_4))

# bms_5 <- lm(cook_pm_log ~ kef, data = unops_bms)
# unops_bms[, bms_5:=exp(predict(bms_5))]
# with(unops_bms, Metrics::rmse(cook_pm, bms_5))

bms_1 <- lm(cook_pm_log ~ ses + kit_vol, data = unops_bms)
unops_bms[, bms_1:=exp(predict(bms_1))]
with(unops_bms, Metrics::rmse(cook_pm, bms_1))

# bms_7 <- lm(cook_pm_log ~ trad_mins, data = unops_bms)
# unops_bms[, bms_7:=exp(predict(bms_7))]
# with(unops_bms, Metrics::rmse(cook_pm, bms_7))

# bms_8 <- lm(cook_pm_log ~ trad_mins + ses, data = unops_bms)
# unops_bms[, bms_8:=exp(predict(bms_8))]
# with(unops_bms, Metrics::rmse(cook_pm, bms_8))

# m9 <- lm(cook_pm_log ~ kit_pm + primary_stove, data = unops_lpg)
# unops_lpg[, m9:=exp(predict(m9))]
# with(unops_lpg, Metrics::rmse(cook_pm, m9))

bms_5 <- lm(cook_pm_log ~ kit_pm + ses + kit_vol, data = unops_bms)
unops_bms[, bms_5:=exp(predict(bms_5))]
with(unops_bms, Metrics::rmse(cook_pm, bms_5))

bms_3 <- lm(cook_pm_log ~ kit_pm + kit_co, data = unops_bms)
unops_bms[, bms_3:=exp(predict(bms_3))]
with(unops_bms, Metrics::rmse(cook_pm, bms_3))

bms_6 <- lm(cook_pm_log ~ kit_pm + kit_co + ses + kit_vol, data = unops_bms)
unops_bms[, bms_6:=exp(predict(bms_6))]
with(unops_bms, Metrics::rmse(cook_pm, bms_6))

# bms_14 <- lm(cook_pm_log ~ kit_pm + cook_co + kit_co + lr_co + kit_vol, data = unops_bms)
# unops_bms[, bms_14:=exp(predict(bms_14))]
# with(unops_bms, Metrics::rmse(cook_pm, bms_14))

# bms_15 <- lm(cook_pm_log ~ bcn_pm_near, data = unops_bms)
# unops_bms[, bms_15:=exp(predict(bms_15))]
# with(unops_bms, Metrics::rmse(cook_pm, bms_15))

bms_4 <- lm(cook_pm_log ~ bcn_pm_near + ses + kit_vol, data = unops_bms)
unops_bms[, bms_4:=exp(predict(bms_4))]
with(unops_bms, Metrics::rmse(cook_pm, bms_4))

model_overview <- function(model, dt){
	form <- formula(get(model))
	adj_r2 <- round(summary(get(model))$adj.r.squared, 2)
	datatable <- get(dt)
	pred <- paste(dt, model, sep="$")
	rmse <- round(Metrics::rmse(datatable$cook_pm, datatable[, get(model)]), 1)
	
	data.table(
		model = model, 
		adj_r2 = adj_r2, 
		rmse = rmse, 
		pred_mean = mean(datatable[, get(model)]),
		pred_sd = sd(datatable[, get(model)]),
		t_test_p = t.test(datatable$cook_pm, datatable[, get(model)])$p.value
	)
}

overview_of_lm_bms <- do.call('rbind', lapply(c(paste('bms_', c(1:6), sep = ""), 'biomass_beacon', 'biomass_no_beacon'), model_overview, 'unops_bms'))
overview_of_lm_bms
# as.data.table(compare_performance(biomass_beacon, biomass_no_beacon, bms_2, bms_3, bms_4, bms_5, bms_6, bms_7, bms_8, bms_10, bms_11, bms_13, bms_14, bms_15, bms_16, bms_17, rank = TRUE))

all_bms_long <- melt.data.table(unops_bms[, c('kef_exposure_predicted', 'cook_pm', 'biomass_beacon', 'biomass_no_beacon', return_cols_like(unops_bms, 'bms_')), with = F], id.var = 'cook_pm')

ggplot(aes(cook_pm, value), data = all_bms_long) + theme_bw() + facet_wrap( ~ variable, scales = 'free_y') + geom_smooth(method = 'lm') + geom_point()


#Summary
unops_ap[, list(mean = mean(cook_pm), sd = sd(cook_pm), n = length(cook_pm))]
unops_ap[, list(mean = mean(cook_pm), sd = sd(cook_pm), n = length(cook_pm)), by = 'primary_stove']
