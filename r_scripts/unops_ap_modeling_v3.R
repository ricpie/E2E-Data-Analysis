options(width=system("tput cols", intern=TRUE))
set.seed(06152020)

library(data.table)
library(ggplot2)

#helper(s)
return_cols_like <- function(dataframe, like, invert = FALSE){
  if(invert){colnames(dataframe)[!colnames(dataframe) %like% like]}else{
  colnames(dataframe)[colnames(dataframe) %like% like]}
}

model_summary <- function(model){
	data.table(Variable = names(coef(get(model))), Coef = coef(get(model)), model = model)
}

rsq <- function (x, y) cor(x, y) ^ 2

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

##############################
###DATA IMPORT AND CLEANING###
##############################
# Import RP's datasets
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

#Import post-placement survey
pp <- as.data.table(readRDS('~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/postplacement_survey.rds'))

str(pp)
pp_subsets <- pp[, return_cols_like(pp, c('D3|D4|D4a|D5|D6|D6a|D7|D8|D8a|D9|D9a|D10|D10-1|D10-2|D10-3|D11|D11a|D12|D13|D13a|D13-1|D13-1a|D14|KeroseneLamp|D15|E1|E2|E2a|Unable to Wear|UnwearableLocation|E2b|WhyUnable|SpecifyWhy|E3|E3-1|E4|E5|E6|E7')), with=F]


#add in stove types
all_merged_summary <- merge(all_merged_summary, stove_types, by = 'HHID')

##############################
##########MODEL PREP##########
##############################
#purge unnecessary columns for LMs
unops_ap <- all_merged_summary[, -c('Start_datetime', 'End_datetime', 'HHIDnumeric', 'quantile', 'emission_startstop','CookingmeanPM25Cook','CookingmeanPM25Kitchen','CookingmeanPM25Kitchen1m','CookingmeanPM25Kitchen2m','CookingmeanPM25LivingRoom','CookingmeanPM25Ambient','CookingmeanCO_ppmCook','CookingmeanCO_ppmKitchen','CookingmeanCO_ppmKitchen1m','CookingmeanCO_ppmKitchen2m','CookingmeanCO_ppmLivingRoom','CookingmeanCO_ppmAmbient','Cookingsum_TraditionalManufactured_minutes','Cookingsum_charcoal_jiko_minutes','Cookingsum_traditional_non_manufactured','Cookingsum_lpg','Cookingmeanpm25_indirect_nearest','Cookingmeanpm25_indirect_nearest_threshold','CookingmeanCO_indirect_nearest','CookingmeanCO_indirect_nearest_threshold', 'countPM25Kitchen', 'countPM25Cook', 'countPATS_LivingRoom', 'countlocation_nearest', 'countlocation_nearestthreshold', 'meanCO_ppmEmissions', 'meanCO2_ppmEmissions','Start.time..Mobenzi.Pre.placement....1', 'na_TraditionalManufactured_minutes', 'na_charcoal_jiko_minutes', 'na_traditional_non_manufactured', 'na_lpg', "stovetype", 'Cookingna_lpg',
	'HHID_full'
	)]

#re-type
unops_ap[, walls_w_eaves_n := as.numeric(as.character(walls_w_eaves_n))]

#log variables to prevent later headaches
unops_ap[, log_meanPM25Cook := log(meanPM25Cook)]
unops_ap[, log_meanPM25Kitchen := log(meanPM25Kitchen)]
unops_ap[, log_meanCO_ppmCook := log(meanCO_ppmCook)]
unops_ap[, log_meanCO_ppmKitchen := log(meanCO_ppmKitchen)]
unops_ap[, wday:=lubridate::wday(Date, label = TRUE)]
unops_ap[, Date := NULL]

str(unops_ap)

#shorter names
setnames(unops_ap, c('hhid', 'cook_pm', 'co2', 'compliance', 'kit_pm', 'lr_pm', 'amb_pm', 'cook_co', 'kit_co', 'lr_co', 'amb_co', 'trad_man_mins', 'jiko_mins', 'trad_non_man_mins', 'lpg_mins', 'bcn_pm_near', 'bcn_pm_thres', 'bcn_pm_thres80', 'bcn_co_near', 'bcn_co_thres', 'walls_with_eaves', 'kit_vol', 'door_win_area', 'ses', 'aer', 'primary_stove', 'cook_pm_log', 'kit_pm_log', 'cook_co_log', 'kit_co_log', 'day_of_week'))

unops_ap[!is.na(jiko_mins), trad_man_mins := jiko_mins]
unops_ap[!is.na(jiko_mins)]

unops_ap[, jiko_mins := NULL]

#additional variables
# unops_ap[primary_stove %like% 'solid fuel', stove_class := 'traditional']
# unops_ap[is.na(stove_class), stove_class := 'lpg']
# unops_ap[, stove_class := as.factor(stove_class)]
unops_ap[, trad_mins := sum(trad_non_man_mins, trad_man_mins, na.rm =T), by = 1:NROW(unops_ap)]
unops_ap[, all_stv_mins := sum(trad_non_man_mins, trad_man_mins, lpg_mins, na.rm = T), by = 1:NROW(unops_ap)]
unops_ap[, primary_stove := as.factor(primary_stove)]

# # old median interpolation for missing values
# unops_ap[is.na(tsiCO2ppm), tsiCO2ppm := unops_ap[, median(tsiCO2ppm, na.rm=T)]]
# unops_ap[is.na(meanPM25LivingRoom), meanPM25LivingRoom := unops_ap[, median(meanPM25LivingRoom, na.rm=T)]]
# unops_ap[is.na(meanPM25Ambient), meanPM25Ambient := unops_ap[, median(meanPM25Ambient, na.rm=T)]]
# unops_ap[is.na(meanCO_ppmCook), meanCO_ppmCook := unops_ap[, median(meanCO_ppmCook, na.rm=T)]]
# unops_ap[is.na(meanCO_ppmKitchen), meanCO_ppmKitchen := unops_ap[, median(meanCO_ppmKitchen, na.rm=T)]]
# unops_ap[is.na(meanCO_ppmLivingRoom), meanCO_ppmLivingRoom := unops_ap[, median(meanCO_ppmLivingRoom, na.rm=T)]]
# unops_ap[is.na(meanCO_ppmAmbient), meanCO_ppmAmbient := unops_ap[, median(meanCO_ppmAmbient, na.rm=T)]]

#updated median interpolation for missing values
unops_interp <- unops_ap[, c('co2', 'lr_pm', 'amb_pm', 'cook_co', 'kit_co', 'lr_co', 'amb_co', 'primary_stove' , 'aer'), with = F]
unops_interp <- melt.data.table(unops_interp, id.var = 'primary_stove')[!is.na(value), list(median = median(value), n_median = length(value)), by = 'primary_stove,variable']

unops_long <- melt.data.table(unops_ap, id.var = c('hhid', 'primary_stove', 'day_of_week'))
unops_long_no_interp <- unops_long[!(is.na(value) & variable %in% c('co2', 'lr_pm', 'amb_pm', 'cook_co', 'kit_co', 'lr_co', 'amb_co', 'primary_stove', 'aer'))]

unops_long_to_interp <- unops_long[is.na(value) & variable %in% c('co2', 'lr_pm', 'amb_pm', 'cook_co', 'kit_co', 'lr_co', 'amb_co', 'primary_stove', 'aer')]
unops_long_to_interp <- merge(unops_long_to_interp, unops_interp[, -c('n_median')], by = c('primary_stove', 'variable'))
unops_long_to_interp[, value := median]
unops_long_to_interp[, median := NULL]
unops_long_interpd <- rbind(unops_long_no_interp, unops_long_to_interp)


##############################
########SUMMARY TABLES########
##############################
#use these frames to generate data summaries
unops_long_summary <- unops_long[!variable %like% "_log"]
unops_long_interpd_summary <- unops_long_interpd[!variable %like% "_log"]

unops_summary_by_stove <- unops_long_summary[,
	list(
		mean = round(mean(value, na.rm = T),2),
		sd = round(sd(value, na.rm = T),2),
		min = round(min(value, na.rm = T),2),
		q25 = round(quantile(value, 0.25, na.rm = T),2),
		median = round(median(value, na.rm=T),2),
		q75 = round(quantile(value, 0.75, na.rm = T),2),
		max = round(max(value, na.rm = T),2),
		n = length(value[!is.na(value)])
		),
	by = 'variable,primary_stove'
]

unops_summary_overall <- unops_long_summary[,
	list(
		mean = round(mean(value, na.rm = T),2),
		sd = round(sd(value, na.rm = T),2),
		min = round(min(value, na.rm = T),2),
		q25 = round(quantile(value, 0.25, na.rm = T),2),
		median = round(median(value, na.rm=T),2),
		q75 = round(quantile(value, 0.75, na.rm = T),2),
		max = round(max(value, na.rm = T),2),
		n = length(value[!is.na(value)])
		),
	by = 'variable'
]

# writexl::write_xlsx(list(overall = unops_summary_overall, by_stove_class = unops_summary_by_stove), '~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Results/model_descriptive_summary_tables.xlsx')

##############################
##########MODEL RUNS##########
##############################
unops_ap <- dcast.data.table(unops_long_interpd, hhid + primary_stove + day_of_week ~ variable, value.var = 'value')
unops_ap <- unops_ap[, -c('kit_pm_log', 'cook_co_log', 'kit_co_log')]
unops_ap[, kef := cook_pm/kit_pm]
unops_ap[primary_stove %like% "LPG", kef_shupler := 30/226] #From Shupler et al 2018 SI
unops_ap[primary_stove %like% "Traditional", kef_shupler := 281/860] #From Shupler et al 2018 SI
unops_ap[primary_stove %like% "Manufactured", kef_shupler := 227/694]
unops_ap[, kef_exposure_shupler := kit_pm * kef_shupler]

unops_ap[, kef_shupler_avg := 0.327]
unops_ap[, kef_exposure_shupler_avg := kit_pm * kef_shupler_avg]

unops_ap[, kef_gbd := 0.742]
unops_ap[, kef_exposure_gbd := kit_pm * kef_gbd]

unops_lpg <- unops_ap[primary_stove %like% 'LPG']
unops_bms <- unops_ap[primary_stove %like% 'Traditional']
unops_chrcl <- unops_ap[primary_stove %like% 'Manufactured']

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
cv_errors

mean_cv_errors <- as.data.table(colMeans(cv_errors, na.rm=TRUE))
mean_cv_errors[, covariates := 1:nrow(mean_cv_errors)]
# qplot(covariates, V1, data=mean_cv_errors[covariates > 3]) + geom_line() + geom_point()

final_subset_lm <-  regsubsets(cook_pm_log ~ primary_stove + all_stv_mins + kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + kit_vol + door_win_area + ses + aer + bcn_pm_near, data = unops_ap, nvmax = n_vars)
coef(final_subset_lm, 7)

unops_ap[, all_beacon := exp(predict_lm_subsets(final_subset_lm, unops_ap, id = 7))]

# ggplot(aes(cook_pm, all_beacon), data=unops_ap) + 
	# geom_point(color = 'coral') + 
	# theme_bw() + 
	# labs(
		# y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
		# x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
	# geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1) + 
	# scale_x_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
	# scale_y_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
	# geom_abline(linetype = 'dashed') 

Metrics::rmse(unops_ap$cook_pm, unops_ap$all_beacon)
all_beacon <- lm(cook_pm_log ~  primary_stove + cook_co + kit_co + lr_co + kit_vol + bcn_pm_near, data = unops_ap)

###########################
########### KEF ###########
###########################

unops_ap_kef <- copy(unops_ap)
unops_ap_kef[primary_stove %like% "LPG", `Primary Stove` := "LPG"]
unops_ap_kef[primary_stove %like% "Manufactured", `Primary Stove` := "Charcoal"]
unops_ap_kef[primary_stove %like% "non-manu", `Primary Stove` := "Traditional Biomass"]

ggplot(aes(kef), data = unops_ap_kef) + 
	geom_density(aes(color = `Primary Stove`, fill = `Primary Stove`), alpha = 0.1) + 
	geom_rug(aes(color = `Primary Stove`))+
	theme_bw() + 
	theme(
		strip.background = element_blank(), 
		strip.text = element_text(face = 'bold'), 
		panel.border = element_blank()
	) +
	labs(x = 'Personal Exposure / Kitchen Concentration') +
	scale_x_log10()

wilcox.test(unops_ap[primary_stove %like% "LPG", kef], unops_ap[!primary_stove %like% "LPG", kef])
t.test(unops_ap[primary_stove %like% "LPG", log(kef)], unops_ap[!primary_stove %like% "LPG", log(kef)])

with(unops_ap, Metrics::rmse(cook_pm, kef_exposure_predicted))
with(unops_ap[primary_stove %like% "LPG"], Metrics::rmse(cook_pm, kef_exposure_predicted))
with(unops_ap[!primary_stove %like% "LPG"], Metrics::rmse(cook_pm, kef_exposure_predicted))

rbind(
	unops_ap_kef[, list(
	Mean = round(mean(kef),2),
	SD = round(sd(kef),2),
	Min = round(min(kef), 2),
	Median = round(median(kef),2),
	Max = round(max(kef), 2)
	), by = 'Primary Stove'],
	unops_ap_kef[, list(
	`Primary Stove` = "All",
	Mean = round(mean(kef),2),
	SD = round(sd(kef),2),
	Min = round(min(kef), 2),
	Median = round(median(kef),2),
	Max = round(max(kef), 2)
	)]
)

ggplot(aes(cook_pm, kef_exposure_shupler), data=unops_ap_kef) + 
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

ggplot(aes(cook_pm, kef_exposure_shupler), data=unops_ap_kef) + 
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
	) + facet_wrap(~`Primary Stove`, scales = 'free')

unops_ap[, Metrics::rmse(cook_pm, kef_exposure_shupler)]
unops_ap[, Metrics::rmse(cook_pm, kef_exposure_shupler_avg)]
unops_ap[, Metrics::rmse(cook_pm, kef_exposure_gbd)]

predicted_kef_exposures <- unops_ap[, c('primary_stove', 'cook_pm', 'kef_exposure_shupler', 'kef_exposure_shupler_avg', 'kef_exposure_gbd'), with = F]
predicted_kef_exposures[, rnum := 1:NROW(predicted_kef_exposures)]
predicted_kefs_summary <- melt(predicted_kef_exposures, id.var = c('rnum', 'primary_stove', 'cook_pm'))[, list(
	r2 = round(summary(lm(cook_pm ~ value))$adj.r.squared, 2),
	RMSE = Metrics::rmse(cook_pm, value),
	mean = mean(value),
	sd = sd (value),
	p = t.test(cook_pm, value)$p.value
	), by = 'variable']

set.seed(rnorm(1)*100)

kef_rmse <- function(n_samples, data){
	if(n_samples>5){
		analysis_data <- copy(data)[primary_stove %like% "LPG" | primary_stove %like% 'non']
	}else{analysis_data <- copy(data)}

	analysis_data[, kef_est:=kit_pm * mean(sample(kef, n_samples))]

	melt(analysis_data[, 
		list(
			rmse = Metrics::rmse(cook_pm, kef_est),
			n = n_samples
		), by = 'primary_stove'
	], id.var = c('n','primary_stove'))
}


kef_rmse_table <- do.call(rbind, parallel::mclapply(rep(1:20,10), kef_rmse, unops_ap))
setorder(kef_rmse_table, n, primary_stove)

kef_rmse_table_summary <- kef_rmse_table[, list(
	mean = mean(value),
	sd = sd(value),
	cov = sd(value)/mean(value)
	), by = 'n,primary_stove,variable']

kef_rmse_table_summary[primary_stove %like% "LPG", `Primary Stove` := "LPG"]
kef_rmse_table_summary[primary_stove %like% "Manufactured", `Primary Stove` := "Charcoal"]
kef_rmse_table_summary[primary_stove %like% "non-manu", `Primary Stove` := "Traditional Biomass"]

ggplot(aes(n, mean), data = kef_rmse_table_summary) + geom_point(aes(color = `Primary Stove`, shape = `Primary Stove`)) +  facet_wrap(~`Primary Stove`, scales='free') + theme_bw()  +	theme(strip.background = element_blank(), strip.text = element_text(face = 'bold')) + guides(shape = FALSE, color = FALSE, fill = FALSE, alpha = FALSE) + labs(x = 'Randomly Selected Number of Measured KEFs', y = 'Average RMSE') + geom_linerange(aes(ymin = mean-sd, ymax = mean+sd, color = `Primary Stove`, shape = `Primary Stove`)) + geom_smooth(method='lm', aes(color = `Primary Stove`, fill = `Primary Stove`), alpha = 0.1)

###########################
########### LMS ###########
###########################
unops_ap[, return_cols_like(unops_ap, "lmf_[0-9]{1,}") := NULL]

#For reviewer2 - Model 0
# lmf_0 <- lm(cook_pm_log ~ primary_stove + ses, data = unops_ap)
# unops_ap[, lmf_0:=exp(predict(lmf_0))]
# with(unops_ap, Metrics::rmse(cook_pm, lmf_0))


#Summary Table Model 1
lmf_1 <- lm(cook_pm_log ~ primary_stove + ses + kit_vol, data = unops_ap)
unops_ap[, lmf_1:=exp(predict(lmf_1))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_1))

#Summary table model 2
lmf_2 <- lm(cook_pm_log ~ kit_pm, data = unops_ap)
unops_ap[, lmf_2:=exp(predict(lmf_2))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_2))

#summary table model 3
lmf_3 <- lm(cook_pm_log ~ kit_pm + kit_co, data = unops_ap)
unops_ap[, lmf_3:=exp(predict(lmf_3))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_3))

#summary table model 4
lmf_4 <- lm(cook_pm_log ~ bcn_pm_near + primary_stove + ses + kit_vol, data = unops_ap)
unops_ap[, lmf_4:=exp(predict(lmf_4))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_4))

#summary table model 5
lmf_5 <- lm(cook_pm_log ~ kit_pm + primary_stove + ses + kit_vol, data = unops_ap)
unops_ap[, lmf_5:=exp(predict(lmf_5))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_5))

#summary table model 6
lmf_6 <- lm(cook_pm_log ~ kit_pm + kit_co + primary_stove + ses + kit_vol, data = unops_ap)
unops_ap[, lmf_6:=exp(predict(lmf_6))]
with(unops_ap, Metrics::rmse(cook_pm, lmf_6))

overview_of_lm_all <- do.call('rbind', lapply(c(paste('lmf_', 1:6, sep = ""), 'all_beacon'), model_overview, 'unops_ap'))
overview_of_lm_all

#for reviewer 2
# overview_of_lm_r2 <- do.call('rbind', lapply(c(paste('lmf_', 0:1, sep = "")), model_overview, 'unops_ap'))
# overview_of_lm_r2


ols_correlations(lmf_1)
ols_correlations(all_beacon)

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

# ggplot(aes(cook_pm, lpg_no_beacon), data=unops_lpg) + 
# 	geom_point(color = 'coral') + 
# 	theme_bw() + 
# 	labs(
# 		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
# 		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
# 	scale_x_continuous(limits = c(0, 150), breaks = seq(0,150,50)) + 
# 	scale_y_continuous(limits = c(0, 150), breaks = seq(0,150,50)) + 
# 	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1) + 
# 	geom_abline(linetype = 'dashed') + 
# 	ggtitle(expression("Relationship between predicted and measured personal exposures to PM"[2.5]))

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
cv_errors
mean_cv_errors <- colMeans(cv_errors, na.rm=TRUE)
plot(sqrt(mean_cv_errors), type = "b")

final_subset_lm <-  regsubsets(cook_pm_log ~ kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + kit_vol + door_win_area + ses + aer + bcn_pm_near, data = unops_lpg, nvmax = n_vars)
coef(final_subset_lm, 6)

unops_lpg[, lpg_beacon := exp(predict_lm_subsets(final_subset_lm, unops_lpg, id = 6))]

# ggplot(aes(cook_pm, lpg_beacon), data=unops_lpg) + 
# 	geom_point(color = 'coral') + 
# 	theme_bw() + 
# 	labs(
# 		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
# 		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
# 	scale_x_continuous(limits = c(0, 150), breaks = seq(0,150,50)) + 
# 	scale_y_continuous(limits = c(0, 150), breaks = seq(0,150,50)) + 
# 	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1) + 
# 	geom_abline(linetype = 'dashed')
Metrics::rmse(unops_lpg$cook_pm, unops_lpg$lpg_beacon)
lpg_beacon <- lm(cook_pm_log ~ lr_pm + cook_co + kit_co + lr_co + ses + bcn_pm_near, data = unops_lpg)

###########################
########### LMS ###########
###########################
unops_lpg[, return_cols_like(unops_lpg, "m[0-9]{1,}") := NULL]

lpg_1 <- lm(cook_pm_log ~ ses + kit_vol, data = as.data.frame(unops_lpg))
unops_lpg[, lpg_1:=exp(predict(lpg_1))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_1))

lpg_2 <- lm(cook_pm_log ~ kit_pm, data = unops_lpg)
unops_lpg[, lpg_2:=exp(predict(lpg_2))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_2))

lpg_3 <- lm(cook_pm_log ~ kit_pm + kit_co, data = unops_lpg)
unops_lpg[, lpg_3:=exp(predict(lpg_3))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_3))

lpg_4 <- lm(cook_pm_log ~ bcn_pm_near + ses + kit_vol, data = unops_lpg)
unops_lpg[, lpg_4:=exp(predict(lpg_4))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_4))

lpg_5 <- lm(cook_pm_log ~ kit_pm + ses + kit_vol, data = unops_lpg)
unops_lpg[, lpg_5:=exp(predict(lpg_5))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_5))

lpg_6 <- lm(cook_pm_log ~ kit_pm + kit_co + ses + kit_vol, data = unops_lpg)
unops_lpg[, lpg_6:=exp(predict(lpg_6))]
with(unops_lpg, Metrics::rmse(cook_pm, lpg_6))

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

# ols_correlations(lm(cook_pm_log ~ ses + kit_vol, data = as.data.frame(unops_lpg)))
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
	best_subset <-  regsubsets(cook_pm_log ~   kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + trad_mins + kit_vol + door_win_area + ses + aer, data = unops_bms[folds != j, ], nvmax = n_vars)

	for(i in 1:n_vars){
  		pred_x <- exp(predict_lm_subsets(best_subset, unops_bms[folds == j, ], id = i))
    	cv_errors[j, i] <- mean((unops_bms$cook_pm[folds == j] - pred_x)^2)
	}
}
cv_errors
mean_cv_errors <- colMeans(cv_errors, na.rm=TRUE)
# plot(mean_cv_errors, type = "b")

final_subset_lm <-  regsubsets(cook_pm_log ~  kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + trad_mins + kit_vol + door_win_area + ses + aer, data = unops_bms, nvmax = n_vars)
coef(final_subset_lm, 7)

unops_bms[, biomass_no_beacon := exp(predict_lm_subsets(final_subset_lm, unops_bms, id = 6))]

# ggplot(aes(cook_pm, biomass_no_beacon), data=unops_bms) + 
# 	geom_point(color = 'coral') + 
# 	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1, color = 'deepskyblue4', fullrange = F) + 
# 	theme_bw() + 
# 	labs(
# 		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
# 		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
# 	scale_x_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
# 	scale_y_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
# 	geom_abline(linetype = 'dashed')

Metrics::rmse(unops_bms$cook_pm, unops_bms$biomass_no_beacon)
biomass_no_beacon <- lm(cook_pm_log ~  lr_pm + cook_co + kit_co + lr_co + kit_vol + ses + aer, data = unops_bms)


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

# ggplot(aes(cook_pm, biomass_beacon), data=unops_bms) + 
# 	geom_point(color = 'coral') + 
# 	geom_smooth(method = 'lm', fill = 'skyblue', alpha = 0.1, color = 'deepskyblue4', fullrange = F) + 
# 	theme_bw() + 
# 	labs(
# 		y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), 
# 		x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + 
# 	scale_x_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
# 	scale_y_continuous(limits = c(0, 700), breaks = seq(0,700,100)) + 
# 	geom_abline(linetype = 'dashed')

Metrics::rmse(unops_bms$cook_pm, unops_bms$biomass_beacon)
biomass_beacon <- lm(cook_pm_log ~ lr_pm + cook_co + kit_co + lr_co + kit_vol + bcn_pm_near, data = unops_bms)

###########################
########### LMS ###########
###########################
unops_bms[, return_cols_like(unops_bms, "bms_") := NULL]

bms_1 <- lm(cook_pm_log ~ ses + kit_vol, data = unops_bms)
unops_bms[, bms_1:=exp(predict(bms_1))]
with(unops_bms, Metrics::rmse(cook_pm, bms_1))

bms_2 <- lm(cook_pm_log ~ kit_pm, data = unops_bms)
unops_bms[, bms_2:=exp(predict(bms_2))]
with(unops_bms, Metrics::rmse(cook_pm, bms_2))

bms_3 <- lm(cook_pm_log ~ kit_pm + kit_co, data = unops_bms)
unops_bms[, bms_3:=exp(predict(bms_3))]
with(unops_bms, Metrics::rmse(cook_pm, bms_3))

bms_4 <- lm(cook_pm_log ~ bcn_pm_near + ses + kit_vol, data = unops_bms)
unops_bms[, bms_4:=exp(predict(bms_4))]
with(unops_bms, Metrics::rmse(cook_pm, bms_4))

bms_5 <- lm(cook_pm_log ~ kit_pm + ses + kit_vol, data = unops_bms)
unops_bms[, bms_5:=exp(predict(bms_5))]
with(unops_bms, Metrics::rmse(cook_pm, bms_5))

bms_6 <- lm(cook_pm_log ~ kit_pm + kit_co + ses + kit_vol, data = unops_bms)
unops_bms[, bms_6:=exp(predict(bms_6))]
with(unops_bms, Metrics::rmse(cook_pm, bms_6))

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

# all_bms_long <- melt.data.table(unops_bms[, c('kef_exposure_predicted', 'cook_pm', 'biomass_beacon', 'biomass_no_beacon', return_cols_like(unops_bms, 'bms_')), with = F], id.var = 'cook_pm')

ols_correlations(bms_1)
ols_correlations(biomass_beacon)

data.table(Variable = names(coef(bms_1)), Coef = coef(bms_1), model)

#models to output coeffs for
models <- c('lmf_1', 'all_beacon', 'lpg_1', 'lpg_beacon', 'bms_1', 'biomass_beacon')
model_summary_out <- do.call(rbind, lapply(models, model_summary))
model_summary_out[model == 'lmf_1', model := "all_1"]
model_summary_out[model == 'all_beacon', model := "all_7"]
model_summary_out[model == 'biomass_beacon', model := "bms_7"]
model_summary_out[model == 'lpg_beacon', model := "lpg_7"]
# write.csv(model_summary_out, '~/Desktop/model_summary_output.csv', row.names = F)

#Summary
unops_ap[, list(mean = mean(cook_pm), sd = sd(cook_pm), n = length(cook_pm))]
unops_ap[, list(mean = mean(cook_pm), sd = sd(cook_pm), n = length(cook_pm)), by = 'primary_stove']

#SL
library(SuperLearner)
sl_libs  <-  c("SL.xgboost", "SL.randomForest", "SL.glmnet", "SL.ksvm",
             "SL.rpartPrune", "SL.mean")

options(mc.cores = 10)
set.seed(06152020, "L'Ecuyer-CMRG")

covars <- c('primary_stove', 'day_of_week', 'co2', 'compliance', 'kit_pm', 'lr_pm', 'amb_pm', 'cook_co', 'kit_co', 'lr_co', 'amb_co', 'walls_with_eaves', 'kit_vol', 'door_win_area', 'ses', 'aer', 'bcn_pm_near', 'bcn_co_near')

# Levels: Cooking gas/LPG stove Manufactured solid fuel stove Traditional solid fuel stove (non-manufactured)
unops_ap[primary_stove %like% "LPG", lpg_stove := 1]
unops_ap[!primary_stove %like% "LPG", lpg_stove := 0]

unops_ap[primary_stove %like% "Manufactured", charcoal_stove := 1]
unops_ap[!primary_stove %like% "Manufactured", charcoal_stove := 0]

unops_ap[primary_stove %like% "non-manu", traditional_biomass_stove := 1]
unops_ap[!primary_stove %like% "non-manu", traditional_biomass_stove := 0]

#Levels: Sun < Mon < Tue < Wed < Thu < Fri < Sat
unops_ap[, day_of_week := as.numeric(day_of_week)]

sl_out <- SuperLearner(Y = unops_ap[,cook_pm], X = unops_ap[, covars, with = F], family = 'gaussian', SL.library = sl_libs)
sl_out

sl_out_2 <- CV.SuperLearner(Y = unops_ap[, cook_pm], X = unops_ap[, covars, with = F], V = 20, family = 'gaussian', SL.library = sl_libs, parallel = 'multicore')
summary(sl_out_2)
sl_out_2$whichDiscreteSL
table(simplify2array(sl_out_2$whichDiscreteSL))

#CV'd model predictions
predictions <- as.data.table(sl_out_2$library.predict)
predictions[, actual := unops_ap[, cook_pm]]
#Ensemble prediction
predictions[, SuperLearner := sl_out_2$SL.predict]
setnames(predictions, c('XGBoost', 'Random Forest', 'Glmnet', 'Ksvm', 'Rpart', 'Mean', 'Measured', 'SuperLearner'))
pred_long <- melt(predictions, id.var = 'Measured')
ggplot(aes(value, Measured), data = pred_long[variable!="Mean"]) + geom_point(aes(shape = variable, color = variable)) + geom_smooth(method = 'lm', aes(color = variable, fill = variable), alpha = 0.2) + theme_bw() + geom_abline(linetype = 'dashed', color = 'blue') + facet_wrap(~variable, ncol = 3) +	theme(strip.background = element_blank(), strip.text = element_text(face = 'bold')) + guides(shape = FALSE, color = FALSE, fill = FALSE, alpha = FALSE) + scale_x_continuous(limits = c(0,700), breaks = seq(0,700,100)) + scale_y_continuous(limits = c(0,700), breaks = seq(0,700,100)) + xlab(expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3})))) + ylab(expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3}))))

Metrics::rmse(predictions$Measured, predictions$XGBoost) 		 
Metrics::rmse(predictions$Measured, predictions$`Random Forest`) 
Metrics::rmse(predictions$Measured, predictions$Glmnet) 		 
Metrics::rmse(predictions$Measured, predictions$Ksvm) 			 
Metrics::rmse(predictions$Measured, predictions$Rpart) 		 	 
Metrics::rmse(predictions$Measured, predictions$SuperLearner) 	 

summary(lm(predictions$Measured ~ predictions$XGBoost))$r.squared  		 
summary(lm(predictions$Measured ~ predictions$`Random Forest`))$r.squared
summary(lm(predictions$Measured ~ predictions$Glmnet))$r.squared 		 
summary(lm(predictions$Measured ~ predictions$Ksvm))$r.squared 			 
summary(lm(predictions$Measured ~ predictions$Rpart))$r.squared  		 
summary(lm(predictions$Measured ~ predictions$SuperLearner))$r.squared  

#review model weights
review_weights  <-  function(cv_sl) {
	meta_weights <- coef(cv_sl)
	means <- colMeans(meta_weights)
	sds <- apply(meta_weights, MARGIN = 2,  FUN = sd)
	mins <- apply(meta_weights, MARGIN = 2, FUN = min)
	maxs <- apply(meta_weights, MARGIN = 2, FUN = max)
	sl_stats <- cbind("mean(weight)" = means, "sd" = sds, "min" = mins, "max" = maxs)
	sl_stats[order(sl_stats[, 1], decreasing = TRUE), ]
}
print(review_weights(sl_out_2), digits = 3)

pred_long[, list(mean = mean(value), sd = sd(value)), by = variable]
unique(pred_long[, t.test(Measured, value), by = 'variable'])


#ICCs
library(ICC)
unops_ap[, stove := as.factor(unique_primary_stove)]

#not generic, but works
icc_calc <- function(var_name){
	output <- as.data.table(ICCest(stove, var_name, data = unops_ap))
	output[, colnames(output) := round(.SD, 2), .SDcols = colnames(output)]
	output[, variable := var_name]
	return(output[, c(1,6,7,8)])
}

var_names <- c('meanPM25Cook', 'meanPM25Kitchen', 'volume', 'door_win_m2', 'score', 'aer')
icc_output <- as.data.table(plyr::ldply(var_names, icc_calc))
icc_output[varw > 1, varw := signif(varw, 2)]
icc_output[vara > 1, vara := signif(vara, 2)]
setnames(icc_output, c('ICC', 'Within-group variance', 'Between-group variance', 'Variable'))
setcolorder(icc_output,c(4, 2, 3, 1))
write.csv(icc_output, '~/Dropbox/UNOPS emissions exposure/Papers/First revision/icc.csv', row.names = FALSE)
