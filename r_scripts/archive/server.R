Sys.setenv(TZ="GMT")
# Sys.setenv(R_ZIPCMD = "/usr/bin/zip")
# Sys.setenv(R_UNZIPCMD = "/usr/bin/unzip")

defaultWD <- getwd()


shinyServer(function(input, output, session) {

	#####################
	### Table Setting ###
	#####################
	setwd(defaultWD)
	Sys.setenv(TZ="UTC")

	datasetInput <- reactive({
		inFile <- as.data.table(input$files)
		print(inFile)
		inFile <- inFile[size>=4000]
		print(inFile)
		#shiny imports as temp file, but stores filename in as part of the object created after upload
		if(is.null(inFile)){
			return(NULL)
		}else if(nrow(inFile)>1){
			inFile_dt <- as.data.table(inFile)
			UPAS_qa_wrapper <- function(x){
				dta <- UPAS_qa(x)
				if(is.null(dta)){
					dta <- NULL
				}else{
					fn <- inFile_dt[datapath==x, name]
					dta[, file:=fn]
				}
				dta
			}			
			UPASData <- as.data.table(ldply(inFile$datapath, UPAS_qa_wrapper))
		}else if(nrow(inFile==1)){
			UPASData <- UPAS_qa(inFile$datapath[1])
			#correct filename
			UPASData[, file:=file_path_sans_ext(inFile$name)]
		}else{NULL}
		UPASData
	})

	datasetOutput <- reactive({
		#require some inputs, otherwise get some ugly warnings
		req(input$files)
		UPASData <- datasetInput()
		#clean up date variables for display
		UPASData[, download_date:=as.character(download_date)]
		UPASData[, qa_date:=as.character(qa_date)]
		UPASData[, hardware_dt:=as.character(hardware_dt)]
		UPASData[, software_dt:=as.character(software_dt)]
		#re-parse filename
		UPASData[, pid:= strsplit(basename(file), "_")[[1]][1]]
		UPASData[, loc:=strsplit(basename(file), "_")[[1]][2]]
		UPASData[, study_phase:=strsplit(basename(file), "_")[[1]][3]]
		UPASData
	})

	rawDatasetOutput <- reactive({
		inFile <- as.data.table(input$files)
		inFile <- inFile[size>=4000]
		#shiny imports as temp file, but stores filename in as part of the object created after uplaod
		if(is.null(inFile)){
			return(NULL)
		}else if(nrow(inFile)>1){
			inFile_dt <- as.data.table(inFile)
			UPAS_ingest_wrapper <- function(x){
				dta <- UPAS_ingest(x, output='raw_data')
				if(is.null(dta)){
					dta <- NULL
				}else{
					fn <- inFile_dt[datapath==x, name]
					dta[, file:=fn]
				}
			}			
			UPASData <- as.data.table(ldply(inFile$datapath, UPAS_ingest_wrapper))
		}else if(nrow(inFile==1)){
			UPASData <- UPAS_ingest(inFile$datapath[1], output='raw_data')
			UPASData[, file:=file_path_sans_ext(inFile$name)]
		}else{NULL}
		UPASData
	})

	selectedRow <- reactive({
		row = input$qaTable_rows_selected
		datasetInput()[row, file]
	})

	qaData <- reactive({
		tblData <- setnames(datasetOutput()[, c('file', 'pid', 'loc', 'study_phase', 'shutdown_reason', "dur", 'flag_total', 'flags')], c('fn', 'HHID', 'Sample Type', 'Phase', "Stop Reason", "Length", "N Flags", "Flags"))
		tblData[, Length:=round(as.numeric(Length),0)]
	})

	output$qaTable <- renderDataTable({
		datatable(qaData(), selection = 'single', options = list( dom = 't', pageLength = 100)) %>% formatStyle('N Flags',  target = 'row',  backgroundColor = styleInterval(0, c('none', 'yellow')))
	})

	output$flowTable <- renderDataTable(
		tblData <- setnames(datasetOutput()[file==selectedRow(), c('file', 'flow_avg', 'flow_sd', 'flow_min', 'flow_max', 'n_flow_deviation', 'percent_flow_deviation', 'flow_flag')], c('fn', 'Mean', "SD", "Min", "Max", "# outside tolerances", 'Frac outside tolerances', 'Flag'))
		, selection = 'none', options = list( dom = 't', pageLength = 100))

	output$nephTable <- renderDataTable(
		tblData <- setnames(datasetOutput()[file==selectedRow(), c('file', 'neph_mean', 'neph_sd', 'neph_min', 'neph_max', 'neph_neg_n', 'neph_percent_neg', 'neph_flag')], c('fn', 'Mean', "SD", "Min", "Max", "# outside tolerances", 'Frac outside tolerances', 'Flag'))
		, selection = 'none', options = list( dom = 't', pageLength = 100))

	output$blTable <- renderDataTable(
		tblData <- setnames(datasetOutput()[file==selectedRow(), c('file', "bl_start", "bl_night", "bl_end", "bl_flag")], c('fn', 'Initial BL', "Nighttime BL", "Final BL", 'Flag'))
		, selection = 'none', options = list( dom = 't', pageLength = 100))

	#####################
	######### UI ######## 
	#####################
	## disabled UI with QA params ##
	output$flowUI <- renderUI({disabled(sliderInput("flow_threshold_ui", label='Acceptable Flow Range', min = 0.275, max = 0.325, value = flow_thresholds))})

	output$sampleLengthUI <- renderUI({disabled(sliderInput("sample_length_ui", label='Acceptable Sample Length Range', min = 1130, max = 1750, value = sample_duration_thresholds))})

	output$blShiftUI <- renderUI({disabled(sliderInput("bl_shift_ui", label='Permissible Baseline Shift', min = 0, max = 100, value = bl_shift_threshold))})

	output$flowCutoffUI <- renderUI({disabled(sliderInput("flow_cutoff_ui", label='Permissible Flow Deviation Fraction', min = 0, max = 1, value = flow_cutoff_threshold))})

	output$nephNegUI <- renderUI({disabled(sliderInput("neph_neg_ui", label='Permissible Neph Percent Neg', min = 0, max = 20, value = neph_neg_threshold))})

	output$nephMagNegUI <- renderUI({disabled(sliderInput("neph_neg_mag_ui", label='Permissible Max Neph Neg', min = -50, max = 0, value = neph_neg_magnitude_threshold))})

	output$ComplianceUI <- renderUI({disabled(sliderInput("compliance_ui", label='Permissible compliance fraction', min = 0, max = 1, value = overall_compliance_threshold))})
	
	output$flagsKey <- renderUI({
		if(is.null(input$files)){
			NULL
		}else{
			HTML('<p><small><em>Flags column key: bl = Baseline Shift • dur = Sample length and/or sample shutdown reason • flow = Flow rate • neph = Nephelometer</em></small></p>')
		}
	})

	output$overviewText <- renderUI({
		if(is.null(input$files)){
			HTML("<h4><strong>Thanks for reviewing your UPAS files.</strong> Start by uploading some files!</h4>")
		}else{
			HTML("<p><strong>Thanks for reviewing your UPAS files.</strong> Select a row in the table below by clicking on it to generate plots and run basic QA-QC on the file (including flow rate, nephelometer reading, baseline drift, and sample length checks). You can output an XLSX of summary and raw data for all of your uploaded files by clicking 'Download' directly below the table.</p>")
		}
	})

	output$docText <- renderUI({
		HTML(paste("
			<p><strong>Overview.</strong> The sidebar contains a list of parameters used to perform quality assurance and quality control checks on uploaded UPAS files. These include checks of the flow rate, the RH-corrected realtime PM readings (from the nephelometer), the total amount of data collected, and the shift in baseline throughout the file.</p>
			<hr>
			<p><strong>Flow Rate Checks.</strong> A flow rate flag is triggered when the percentage of values outside of the acceptable flow range (",flow_thresholds[1]," - ", flow_thresholds[2], " lpm) exceeds a set amount (", flow_cutoff_threshold*100, "% of all flows measured).</p>
			<p><strong>Nephelometer Checks.</strong> A nephelometer flag is triggered either when the percentage of negative values exceeds a set amount (", neph_neg_threshold, "% of the total sample) or when any nephelometer value is less than or equal to ", neph_neg_magnitude_threshold,".</p>
			<p><strong>Sample Duration Checks. </strong> Two sample duration flags can be triggered. The first is triggered when when the sample length is outside of the acceptable  range (",sample_duration_thresholds[1]," - ", sample_duration_thresholds[2], " minutes). The second is triggered if the 'Shutdown Reason' includes any reason other than 'Duration Stop.'</p>
			<p><strong>Baseline Shift Checks. </strong> Baseline shift is assessed by first calculating the  5th percentile of the distribution of nephelometer values for every hour of sampling. Three periods are selected to assess changes in baseline -- the first three hours of sampling, the last three of sampling and a night period (between 23:00 and 1:00). If any of the absolute values of the median value from these periods varies by ", bl_shift_threshold, " or greater, a flag is triggered. </p>
			<p><strong>Compliance Checks. </strong> Compliance is assessed by first calculating the ", window_width ,"-sample rolling standard deviation of the vector composite (xyz) acceleration from the minute averaged data. The sum of samples exceeding ", bl_shift_threshold, " is divided by the total number of samples, and to be in compliance a fraction higher than ", overall_compliance_threshold ," should be achieved. </p>
			", sep=""))
	})

	output$plotTabBox <- renderUI({
		if(is.null(input$files)){
			NULL}else
		if(is.null(input$qaTable_rows_selected)){
			return(NULL)
		}else{
	    	tabBox(width = 12,
        		id = 'plots',
        		title = "Plots", 
				tabPanel("Nephelometer", dygraphOutput("nephelometerPlot", height = '250px')),
				tabPanel("Flow Rates", dygraphOutput("flowratePlot", height = '250px')),
				tabPanel("Motion Plot", dygraphOutput("accelPlot", height = '250px')),
				tabPanel("Baseline Plot", 
					p("5th percentile of the distribution of nephelometer values for each hour of the file."),
					dygraphOutput("baselinePlot", height = '250px')
					)
        	)
		}
	})

	output$tableTabBox <- renderUI({
		if(is.null(input$files)){
			NULL}else
		if(is.null(input$qaTable_rows_selected)){
			return(NULL)
		}else{
	    	tabBox(width = 12,
        		id = 'plots',
        		title = "Summary Tables", 
        		tabPanel("Nephelometer Summary", div(style = 'overflow-x: scroll', dataTableOutput("nephTable"))),
				tabPanel("Flow Summary", div(style = 'overflow-x: scroll', dataTableOutput("flowTable"))),
				tabPanel("Baseline Summary", div(style = 'overflow-x: scroll', dataTableOutput("blTable")))
        	)
		}
	})

	output$downloadButton <- renderUI({
		if(is.null(input$files)){
			NULL
		}else{
			downloadLink('downloadArchive', label = "Download")
		}
	})	
	
	#####################
	####### PLOTS ####### 
	#####################
	### AND PLOT DATA ###
	#####################
	plotData <- reactive({
		req(input$files)
		if(is.null(input$qaTable_rows_selected)){
			return(NULL)
		}else{
			data_for_plot <- rawDatasetOutput()[file==selectedRow()]
			data_for_plot
		}
	})

	output$nephelometerPlot = renderDygraph({
		req(input$files)
		if(is.null(input$qaTable_rows_selected)){
			return(NULL)
		}else{
			dygraph(as.xts.data.table(plotData()[!is.na(rh_cor_neph), c('datetime', 'rh_cor_neph')])) %>% 
			    dyOptions(axisLineWidth = 1.5, fillGraph = T, drawGrid = TRUE, colors = RColorBrewer::brewer.pal(4, "Set2"), labelsUTC = TRUE)
		}
	})

	output$flowratePlot = renderDygraph({
		req(input$files)
		if(is.null(input$qaTable_rows_selected)){
			return(NULL)
		}else{
			dygraph(as.xts.data.table(plotData()[!is.na(flow), c('datetime', 'flow')])) %>% 
			    dyOptions(axisLineWidth = 1.5, fillGraph = F, drawGrid = TRUE, colors = RColorBrewer::brewer.pal(4, "Set1"), labelsUTC = TRUE)
		}
	})

	output$accelPlot = renderDygraph({
		req(input$files)
		if(is.null(input$qaTable_rows_selected) | nrow(plotData()[!is.na(acc_comp)]) == 0){
			return(NULL)
		}else{
			dygraph(as.xts.data.table(plotData()[!is.na(acc_comp), c('datetime', 'acc_comp')])) %>% 
			    dyOptions(axisLineWidth = 1.5, fillGraph = F, drawGrid = TRUE, colors = RColorBrewer::brewer.pal(2, "Set3"), labelsUTC = TRUE)
		}
	})

	blPlotData <- reactive({
		req(input$files)
		if(is.null(input$qaTable_rows_selected)){
			return(NULL)
		}else{
			data_for_plot2 <- rawDatasetOutput()[file==selectedRow() & !is.na(rh_cor_neph), quantile(rh_cor_neph, 0.05), by=round_date(datetime, 'hour')]
			data_for_plot2
		}
	})

	output$baselinePlot = renderDygraph({
		req(input$files)
		if(is.null(input$qaTable_rows_selected)){
			return(NULL)
		}else{
			dygraph(as.xts.data.table(blPlotData()[, c('round_date', 'V1')])) %>% 
			    dyOptions(axisLineWidth = 1.5, fillGraph = F, drawGrid = TRUE, colors = RColorBrewer::brewer.pal(1, "Set1"), labelsUTC = TRUE)
		}
	})

	##########################
	####### Downloads ########
	##########################
	output$downloadArchive <- downloadHandler(
	    filename = function() {paste("UPAS_QA_OUTPUT", "_", gsub("-","",Sys.Date()), ".zip", sep = '')},
	    content = function(fname) {
			tmpdir <- tempdir()
			setwd(tmpdir)
			fs = c("summary_output.xlsx")
			write_xlsx(list('summary' = datasetOutput(), 'raw_data' = rawDatasetOutput()), path = paste(tmpdir, "summary_output.xlsx", sep="/"))
			zip(zipfile=fname, files=fs)
			setwd(defaultWD)
		},
		contentType = "application/zip"
	)
})