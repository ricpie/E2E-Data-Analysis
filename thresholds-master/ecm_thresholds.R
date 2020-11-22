#set QA-QC parameters
flow_thresholds <- c(0.25, 0.35)
flow_cutoff_threshold <- 0.10
flow_rolling_sd_threshold <-  0.2
flow_min_threshold <- 0.10
flow_max_threshold <- 0.50	

sample_duration_thresholds <- c(1440*.90,1440*1.10)

bl_shift_threshold <- 30

neph_neg_threshold <- 20
neph_neg_magnitude_threshold <- -20

inlet_pressure_threshold <- 5

temp_thresholds <- c(0, 50)

rh_threshold <- 85

accel_compliance_threshold <- 0.02 #Compliance flag
window_width <- 20
overall_compliance_threshold <- 0.2 #If more than this fraction of samples is not categorize as being compliant, run is flagged.  Includes nighttime data.
