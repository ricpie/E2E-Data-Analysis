# unops_ap[, sum_TraditionalManufactured_minutes:=as.numeric(sum_TraditionalManufactured_minutes)]
# unops_ap[, sum_TraditionalManufactured_minutes:=as.numeric(sum_TraditionalManufactured_minutes)]
# unops_ap[, sum_charcoal_jiko_minutes:=as.numeric(sum_charcoal_jiko_minutes)]
# unops_ap[, sum_traditional_non_manufactured:=as.numeric(sum_traditional_non_manufactured)]
# unops_ap[, sum_lpg:=as.numeric(sum_lpg)]

# unops_ap[sum_TraditionalManufactured_minutes==0, sum_TraditionalManufactured_minutes:=0.001]
# unops_ap[sum_TraditionalManufactured_minutes == 0, sum_TraditionalManufactured_minutes := 0.001]
# unops_ap[sum_charcoal_jiko_minutes == 0, sum_charcoal_jiko_minutes := 0.001]
# unops_ap[sum_traditional_non_manufactured == 0, sum_traditional_non_manufactured := 0.001]
# unops_ap[sum_lpg == 0, sum_lpg := 0.001]

# unops_ap[meanCO_ppmKitchen == 0, meanCO_ppmKitchen := unops_ap[meanCO_ppmKitchen!=0, min(meanCO_ppmKitchen, na.rm=T)]]
# unops_ap[meanCO_ppmLivingRoom == 0, meanCO_ppmLivingRoom := unops_ap[meanCO_ppmLivingRoom!=0, min(meanCO_ppmLivingRoom, na.rm=T)]]
# unops_ap[meanCO_ppmAmbient == 0, meanCO_ppmAmbient := unops_ap[meanCO_ppmAmbient!=0, min(meanCO_ppmAmbient, na.rm=T)]]
# unops_ap[meanCO_indirect_nearest == 0, meanCO_indirect_nearest := unops_ap[meanCO_indirect_nearest!=0, min(meanCO_indirect_nearest, na.rm=T)]]
# unops_ap[meanCO_indirect_nearest_threshold ==0, meanCO_indirect_nearest_threshold := unops_ap[meanCO_indirect_nearest_threshold!=0, min(meanCO_indirect_nearest_threshold, na.rm=T)]]

#shift the 0 sums to 0.0001

#kitchen sink
# k <- lm(cook_pm_log ~ co2 + kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + amb_co + trad_mins + lpg_mins + walls_with_eaves + kit_vol + door_win_area + ses + aer + primary_stove + day_of_week, data = unops_ap)
# summary(k)
# model_performance(k)
# check_model(k)

# k_optimum <- ols_step_best_subset(k)
# alarm()
# alarm()
# alarm()
# alarm()
# alarm()
# alarm()
# alarm()
# alarm()
# alarm()


#unops scratch
m1 <- lm(log_meanPM25Cook ~ unique_primary_stove, data = unops_log)
model_performance(m1)
# check_model(m1)

m2 <- lm(log(meanPM25Cook) ~ unique_primary_stove + score, data = unops_log)
model_performance(m2)
# check_model(m2)

m3 <- lm(log(meanPM25Cook) ~ log(meanPM25Kitchen), data = unops_log)
model_performance(m3)

m4 <- lm(log(meanPM25Cook) ~ log(meanPM25Kitchen) + unique_primary_stove, data = unops_log)
model_performance(m4)

m5 <- lm(log(meanPM25Cook) ~ log(meanPM25Kitchen) + unique_primary_stove + score, data = unops_log)
model_performance(m5)

m6 <- lm(log(meanPM25Cook) ~ log(meanPM25Kitchen) + unique_primary_stove + score + aer, data = unops_log)
model_performance(m6)

m7 <- lm(log(meanPM25Cook) ~ log(meanPM25Kitchen) + unique_primary_stove + score + walls_w_eaves_n + volume + door_win_m2, data = unops_log)
model_performance(m7)

compare_performance(m1, m2, m3, m4, m5, m6, m7, rank = TRUE)

m8 <- lm(log(meanPM25Cook) ~ unique_primary_stove + score + walls_w_eaves_n + volume + door_win_m2 +sum_TraditionalManufactured_minutes + sum_traditional_non_manufactured, data = all_merged_summary)
model_performance(m8)
summary(m8)

m9 <- lm(log(meanPM25Cook) ~ log(meanpm25_indirect_nearest), data = all_merged_summary)
summary(m9)

m10 <- lm(log(meanPM25Cook) ~ log(meanpm25_indirect_nearest_threshold) + unique_primary_stove + score , data = all_merged_summary)
summary(m10)

m11 <- lm(log(meanPM25Cook) ~ log(meanCO_ppmCook) + unique_primary_stove + aer + score, data = all_merged_summary)
summary(m11)


m12 <- lm(log_meanPM24Cook ~ log_meanPM25Kitchen + log_meanCO_ppmCook + log_meanpm25_indirect_nearest_threshold80 + walls_w_eaves_n + volume + door_win_m2 + score + aer + unique_primary_stove, data = all_merged_summary)
k <- ols_step_all_possible(m12)
plot(ols_step_best_subset(m12))





cv_lm <- function(fold, data, reg_form) {
  # get name and index of outcome variable from regression formula
  out_var <- as.character(unlist(str_split(reg_form, " "))[1])
  out_var_ind <- as.numeric(which(colnames(data) == out_var))

  # split up data into training and validation sets
  train_data <- training(data)
  valid_data <- validation(data)

  # fit linear model on training set and predict on validation set
  mod <- lm(as.formula(reg_form), data = train_data)
  preds <- predict(mod, newdata = valid_data)

  # capture results to be returned as output
  out <- list(coef = data.frame(t(coef(mod))),
              SE = ((preds - valid_data[, out_var_ind])^2))
  return(out)
}

library(origami)
all_merged_summary[, log_meanPM24Cook := log(meanPM25Cook)]
folds <- make_folds(as.data.frame(all_merged_summary))
cvlm_results <- cross_validate(cv_fun = cv_lm, folds = folds, data = as.data.frame(all_merged_summary),
                               reg_form = "log_meanPM24Cook ~ unique_primary_stove + score + aer")
mean(cvlm_results$SE, na.rm=TRUE)
# check_model(m1)




#separate frames for datastreams
pm_pollution <- c('PATS_1m', 'PATS_2m', 'PATS_Kitchen', 'PATS_LivingRoom', 'PATS_Ambient', 'pm_compliant', 'PM25Cook', 'PM25Kitchen', 'ECM_kitchen', 'pm25_conc_beacon_nearest_ecm', 'pm25_conc_beacon_nearestthreshold_ecm', 'pm25_conc_beacon_nearestthreshold_ecm80')
co_pollution <-  c('CO_ppm1m', 'CO_ppm2m', 'CO_ppmCook', 'CO_ppmKitchen', 'CO_ppmLivingRoom', 'CO_ppmAmbient', 'co_estimate_beacon_nearest', 'co_estimate_beacon_nearest_threshold')
metadata <- c('datetime', 'date', 'HHID', 'HHIDnumeric', 'stovetype', 'pm_primary_stove')
sums <- c('sumstraditional_non_manufactured', 'sumslpg', 'sumstraditional_manufactured', 'sumscharcoal.jiko')

#drop wonky household, spare pain
unops <- unops[HHID!='KE238-KE06']

### CRONCHY DATA TABLES MMM
#pm
pm <- unops[, c(metadata, pm_pollution), with = F]
pm_long <- melt(pm, id.var = c('datetime', 'date', 'HHID', 'HHIDnumeric', 'stovetype', 'pm_primary_stove', 'pm_compliant'))
pm_long[, rollmean := frollmean(value, n = 15), by = 'HHID,HHIDnumeric,pm_primary_stove,variable']

pm_long_summary <- pm_long[, list(
	compliance = length(pm_compliant[!is.na(pm_compliant)]),
	avg = mean(value, na.rm = T),
	max_15m = max(rollmean),
	sd = sd(value, na.rm = T),
	n = length(value[!is.na(value)]),
	start = min(datetime),
	stop = max(datetime)
	), by = 'HHID,HHIDnumeric,pm_primary_stove,variable']

setkey(pm_long_summary, HHID, date)

pm_long_summary[, c(NA, diff(date)), by ='HHID'][V1>1]
pm_long_summary[HHID == 'KE238-KE06']

pm_summary_wide <- dcast.data.table(pm_long_summary, HHID + HHIDnumeric  + pm_primary_stove + compliance + start + stop ~ variable, value.var = c('avg', 'max_15m', 'sd', 'n'))
#fin

#co
co <- unops[, c(metadata, co_pollution), with = F]
co_long <- melt(co, id.var = c('datetime', 'date', 'HHID', 'HHIDnumeric', 'stovetype', 'pm_primary_stove'))
co_long[, rollmean := frollmean(value, n = 15), by = 'HHID,HHIDnumeric,pm_primary_stove,variable']

co_long_summary <- co_long[, list(
	avg = mean(value, na.rm = T),
	max_15m = max(rollmean),
	sd = sd(value, na.rm = T),
	n = length(value[!is.na(value)]),
	start = min(datetime),
	stop = max(datetime)
	), by = 'HHID,HHIDnumeric,pm_primary_stove,variable']

setkey(co_long_summary, HHID, date)
co_long_summary[, c(NA, diff(date)), by ='HHID'][V1>1]

co_summary_wide <- dcast.data.table(co_long_summary, HHID + HHIDnumeric + pm_primary_stove + start + stop ~ variable, value.var = c('avg', 'max_15m', 'sd', 'n'))
#fin


###SUMS
#extract variable
sums <- unops[, c(metadata, sums), with = F]
#make long
sums_long <- melt(sums, id.var = c('datetime', 'date', 'HHID', 'HHIDnumeric', 'stovetype', 'pm_primary_stove'))
#turn boolean into numeric
sums_long[, value:=as.numeric(value)]
#unique stoves
sums_long[, unique(pm_primary_stove)]
#make long summary
sums_long_summary <- sums_long[, list(
	mins = sum(value, na.rm = T), 
	n = length(value[!is.na(value)]),
	start = min(datetime),
	stop = max(datetime)
	), by = 'HHID,HHIDnumeric,pm_primary_stove,variable']
#make wide summary
sums_wide_summary <- dcast.data.table(sums_long_summary, HHID + HHIDnumeric + pm_primary_stove + start + stop ~ variable, value.var = c("mins", "n"))
#fin

# remerge all
setkey(pm_summary_wide)
setkey(co_summary_wide)
setkey(sums_wide_summary)
unops_ap <- merge(merge(pm_summary_wide, co_summary_wide), sums_wide_summary)[HHID!='KE238-KE06']

# SES
ses <- as.data.table(readRDS('~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/predicted_ses.rds'))
ses[, id := NULL]
merge(unops_ap, ses, by = "HHID", all.x = T)


corr_var(unops_summary[,-c('Date','Start_datetime','End_datetime','meanPM25Cook')], # name of dataset
  cook_log, # name of variable to focus on
  top = 25, logs = T # display top 5 correlations
) 


#impute by median

#model hierarchy
#PE ~ stove type / fuel type only
#PE ~ stove type + survey data only
#PE ~ kitchen PM
#PE ~ kitchen PM * 0.742
#PE ~ kitchen CO
#PE ~ kitchen PM + kitchen CO
# ...
#PE ~ beacon
#PE ~ beacon + survey

# SES score - keep it valued
# For all 2000 households
# all major assets, household features (owning vs renting), housing characteristics
# emissions - volume, eaves
# 

# meta emissions
# lascar 1.5m monitor
# supplementing with TSI if available
# HHID-full

#all_merged_summary

#build linear models. plug in values for long term monitoring. around 20 households were intensive.
#compare single measure to long term measure.

#kitchen * .742; compare with personal

# meanpm25_indirect_nearest
# meanpm25_indirect_nearest_threshold - didn't perform as well as threshold 80. The more loose algorithm performed better than the more strict one. 
# meanCO_indirect_nearest
# meanCO_indirect_nearest_threshold

#coefficients R2s


# overview <- melt.data.table(
# 	data.table(
# 		predictors = 1:19,
# 	    `Adj R2` = summary(final_subset_lm)$adjr2,
# 	    Cp = summary(final_subset_lm)$cp,
#     	BIC = summary(final_subset_lm)$bic
#     ), id.var = 'predictors') 

# ggplot(aes(predictors, value), data = overview) + 
# 	geom_line(aes(color = variable), show.legend = F) + 
# 	theme_bw() + 
# 	geom_point(aes(shape = variable, color = variable), show.legend = F) + 
# 	facet_wrap( ~ variable, ncol = 4, scales = 'free_y') + 
# 	theme_bw() + 
# 	theme(
# 		strip.background = element_blank(), 
# 		strip.text = element_text(face = 'bold'), 
# 		panel.border = element_blank()
# 	) +
# 	labs(x = 'Number of Predictors', y = 'Value')



for(j in 1:k){
	best_subset <-  regsubsets(cook_pm ~ primary_stove + co2 + kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + amb_co + bcn_pm_thres80 + trad_mins + lpg_mins + walls_with_eaves + kit_vol + door_win_area + ses + aer + day_of_week, data = unops_ap[folds != j, ], nvmax = 22)

	for(i in 1:n_vars){
  		pred_x <- predict.regsubsets(best_subset, unops_ap[folds == j, ], id = i)
    	cv_errors[j, i] <- mean((unops_ap$cook_pm_log[folds == j] - pred_x)^2)
	}
}

mean_cv_errors <- colMeans(cv_errors, na.rm=TRUE)
plot(mean_cv_errors, type = "b")

final_subset <-  regsubsets(cook_pm ~ primary_stove + co2 + kit_pm + lr_pm + amb_pm + cook_co + kit_co + lr_co + amb_co + bcn_pm_thres80 + trad_mins + lpg_mins + walls_with_eaves + kit_vol + door_win_area + ses + aer + day_of_week, data = unops_ap, nvmax = 22)

coef(final_subset, 6)
summary(lm(cook_pm ~ primary_stove + cook_co + kit_co + lr_co + bcn_pm_thres80 + kit_vol, data = unops_ap))

overview <- melt.data.table(
	data.table(
		predictors = 1:22,
	    `Adj R2` = summary(final_subset)$adjr2,
	    Cp = summary(final_subset)$cp,
    	BIC = summary(final_subset)$bic
    ), id.var = 'predictors') 

ggplot(aes(predictors, value), data = overview) + geom_line(aes(color = variable), show.legend = F) + theme_bw() + geom_point(aes(shape = variable, color = variable), show.legend = F) + facet_wrap( ~ variable, ncol = 4, scales = 'free_y') + 	theme_bw() + 
	theme(strip.background = element_blank(), strip.text = element_text(face = 'bold'), panel.border = element_blank()) +
	labs(x = 'Number of Predictors', y = 'Value')

unops_ap[, predicted := exp(predict.regsubsets(final_subset, unops_ap, id = 7))]
unops_ap[, predicted2 := exp(predict.regsubsets(final_subset, unops_ap, id = 22))]

with(unops_ap, Metrics::rmse(cook_pm, predicted2))

ggplot(aes(cook_pm, predicted), data=unops_ap) + geom_point() + theme_bw() + labs(y = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), x = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + scale_x_log10() + scale_y_log10() + geom_smooth(method = 'lm') + geom_abline(linetype = 'dashed') + ggtitle(expression("Relationship between predicted and measured personal exposures to PM"[2.5]))

ggplot(aes(cook_pm, predicted), data=unops_ap) + geom_point() + theme_bw() + labs(x = expression(paste("Predicted PM"[2.5], " Exposure ", ("μg/m"^{3}))), y = expression(paste("Measured PM"[2.5], " Exposure ", ("μg/m"^{3})))) + scale_x_continuous(limits = c(0, 700), breaks = seq(0,700,50)) + scale_y_continuous(limits = c(0, 700), breaks = seq(0,700,50)) + geom_smooth(method = 'lm') + geom_abline(linetype = 'dashed')