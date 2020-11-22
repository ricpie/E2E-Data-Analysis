### Ajay Pillarisetti, University of California, Berkeley, 2016 - 2018
### V1. For UPAS by Ricardo Piedrahita 2019
function(request){
	dashboardPage(
	  dashboardHeader(title = "UPAS QAQC", titleWidth = 350),
	  dashboardSidebar(disable=F, width = 350,
	    sidebarMenu(id = "tabs", 
			fileInput(
				"files",
				HTML("<strong>Upload one or more UPAS files.</strong><br>"), 
				multiple = T, 
				accept = ".csv"),
			HTML("<div style='margin-left:20px; margin-top:0px;'><small>UPAS files should be named as follows:  HHID_MONITORENV_<br>REPLICATE_DEVICEID_FILTERID_DDMMYYYY.csv</small></div>"),
			HTML("<hr>"),
			HTML("<span style='margin-left:15px; font-size:16px;'><strong>QA Thresholds (non-editable)</strong></span>"),
			uiOutput('flowUI'),
			uiOutput('flowCutoffUI'),
			HTML("<hr>"),
			uiOutput('sampleLengthUI'),
			HTML("<hr>"),
			uiOutput('blShiftUI'),
			HTML("<hr>"),
			uiOutput('ComplianceUI')
	    )
	  ),
	    dashboardBody(
	        useShinyjs(),
	        tags$head(
	            tags$link(rel = "stylesheet", type = "text/css", href = "custom.css"),
                # tags$link(rel = "stylesheet", href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/3.5.2/animate.min.css"),
	            tags$script(HTML('Shiny.addCustomMessageHandler("jsCode",function(message) {eval(message.value);});'))            
	            ),
	        fluidRow(
	        	box(width = 12,
	        		id = 'qa_params',
	        		title = "Overview", 
	        		htmlOutput('overviewText'),
	        		div(style = 'overflow-x: scroll', 
	        			dataTableOutput("qaTable"),
		        		uiOutput('flagsKey')
					),
	        		uiOutput('downloadButton')
	        	),
				uiOutput('tableTabBox'),
	        	uiOutput('plotTabBox'),
	        	box(width = 12,
	        		id = 'documentation',
	        		title = "Documentation",
	        		htmlOutput('docText')
	        	),
    			HTML("
				<div style='margin: 20px; text-align:center;'>
					<p><small><em> And you may find yourself //
						 Behind the screen of a large computator <br>
						 And you may find yourself with many many sensors //
						 With much too much data <br>
						 And you may ask yourself, well //
						 How did I get here?
					</em></small></p>
					<p>Built by Jiawen Liao, Wenlu Ye, Ricardo Piedrahita, and Ajay Pillarisetti</p>
				</div>
			")

	        )
	    )
	)
}
