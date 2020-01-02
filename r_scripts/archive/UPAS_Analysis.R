#Code by Matt Shupler. Modified by Ricardo Piedrahita August 12 2019
# install.packages("mailR", repos="http://cran.rstudio.com/")
library(mailR)  ## need to install the most updated version of JAVA
# install.packages("plyr", repos="http://cran.rstudio.com/")
library(plyr)
source('UPAS Code/functions.R')
TodayDate=Sys.Date()
errorpath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/UPAS Code/ErrorFiles/ErrorFiles-"
datafilepath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/UPAS Code/Test Files UPAS"
correctfilepath <- "~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/UPAS Code/CorrectFiles/"

UPASfiles=list.files(datafilepath,full.names=T)
#UPASfiles2=as.data.frame(UPASfiles)
#UPASfilesNewFirmware=subset(UPASfiles2, substr(UPASfiles,58,60)=="UTC")

#remove the files folder

#files already checked
#load in file with all the files that have already been checked (run through the loops for errors)
AlreadyChecked=NULL
#AlreadyChecked = load("/Users/ricardopiedrahita/Dropbox/UNOPS emissions exposure/E2E Data Analysis/UPAS Code/AlreadyChecked.Rdata")
ACread = NULL
ACread=readRDS(paste0(datafilepath,'/AlreadyChecked.rds'))
ACread2=subset(ACread, substr(AlreadyChecked))
ACread3=as.vector(ACread['AlreadyChecked'])

#AlreadyChecked2=NA
#AlreadyChecked[1]=paste("C:/Research/Remote/Uploads/",substr(AlreadyChecked[1],64,130),sep="")

Errors=data.frame(FileName=as.character(),
                  shutdown_reason=as.numeric(),
                  StartBatteryCharge=as.numeric(),
                  EndBatteryCharge=as.numeric(),
                  StartBatteryVoltage=as.numeric(),
                  EndBatteryVoltage=as.numeric(),
                  SampledVolumeHeader=as.numeric(),
                  SampledRuntimeHeader= as.numeric(),
                  AvgFlow= as.numeric(),
                  TotalVolume=as.numeric(),
                  FlowRate=as.numeric(),
                  SampleLength=as.numeric(),
                  Problem=as.character(), stringsAsFactors = F)

Info=data.frame(file.info(UPASfiles))
Info$Name=row.names(Info)
Info15=Info[,c("Name","size", "mtime")]
Info15$Letters=nchar(as.character(Info15$Name))
Info16=subset(Info15, Letters>46)
Info16$FinalLetters=substr(Info16$Name,nchar(Info16$Name)-6,nchar(Info16$Name)-4)
Info2=subset(Info16, FinalLetters!="___" & FinalLetters!="hhh" & FinalLetters!="HHH" & FinalLetters!="---")
#InfoFinal=subset(Info16,FinalLetters=="---")

Info2$NoData=0
Info3=as.data.frame(Info2[,1])

names(Info3)[1]="AlreadyChecked"

CheckedInfo=rbind(Info3,ACread)

CheckedInfo2=CheckedInfo

DuplicateFiles2=duplicated(CheckedInfo2)
Duplicate3=cbind(CheckedInfo2,DuplicateFiles2)
Duplicate4=subset(Duplicate3, DuplicateFiles2=="TRUE")
Duplicate5=merge(Duplicate4,NonDuplicate1,by="AlreadyChecked",all.y=T)

NonDuplicate=subset(Duplicate3, DuplicateFiles2=="FALSE")
NonDuplicate1=subset(NonDuplicate, select=-DuplicateFiles2)
NonDuplicate2=subset(Duplicate5, is.na(DuplicateFiles2))
NonDuplicate2$SubName1=substr(NonDuplicate2$AlreadyChecked,58,63)
NonDuplicate2$SubName2=substr(NonDuplicate2$AlreadyChecked,65,70)
NonDuplicate2$Filter=substr(NonDuplicate2$AlreadyChecked,79,84)

#account for some field staff not correctly entering the date into the UPAS
numbers_only <- function(x) !grepl("\\D", x)

NonDuplicate2$CheckNumber=NULL

for (i in (1:nrow(NonDuplicate2))){
  
  if(substr(NonDuplicate2$SubName1[i],1,3)!="UTC"){NonDuplicate2$CheckNumber[i]=numbers_only(NonDuplicate2$SubName2[i])}
  else if(substr(NonDuplicate2$SubName1[i],1,3)=="UTC") {NonDuplicate2$CheckNumber[i]=FALSE}
}


NonDuplicate3=subset(NonDuplicate2, SubName1!="000000" | SubName1!="------" | SubName1!="23____" | (SubName1=="000000" & CheckNumber=="TRUE"))
NonDuplicate3=subset(NonDuplicate2, SubName1!="000000" | (SubName1=="000000" & CheckNumber=="TRUE"))
NonDuplicate31=subset(NonDuplicate3,SubName2!="------" & SubName2!="23____" & SubName2!="3~____" & SubName1!="UTC_te" & SubName2!="000011")
NonDuplicate32=subset(NonDuplicate31, substr(SubName2, 1,3)!="Ref")

NonDuplicate32$NewFirmwareDate=substr(NonDuplicate32$AlreadyChecked,39,48)

NonDuplicate32$NewFirmwareDate2=NULL
for (i in (1:nrow(NonDuplicate32))){NonDuplicate32$NewFirmwareDate2[i]=ifelse(substr(NonDuplicate32$AlreadyChecked[i],58,60)=="UTC",
                                                                             as.Date(NonDuplicate32$NewFirmwareDate[i], format="%Y-%m-%d"),NA)}

NonDuplicate4=subset(NonDuplicate33, SubName2!="upasvs" & nchar(SubName2)>5 & Filter!="filter")

#file path containing all files newly added to server
NewFiles=as.vector(NonDuplicate4$AlreadyChecked)

NewFilesOut=data.frame(file.info(NewFiles))

if (nrow(NewFilesOutput)==0){
 
  CountProblems=paste("Dear all, The code ran at", Sys.time(), 
                      "You have 0 runs with no errors and 0
                      runs with at least one error")
  
  #send in an email
  send_mail_function(CountProblems)
  
  stop("the script must end here")
   
} else if (nrow(NewFilesOutput)>0) {NewFilesOutput$NoData=0}


NewFilesOutput2=NewFilesOutput[order(NewFilesOutput$size),]

for (i in 1:nrow(NewFilesOutput)){
  
  if ((NewFilesOutput$size[i] <2000 | is.na(NewFilesOutput$size[i])) & nchar(as.character(NewFilesOutput$Name[i]))>46){
    
    NewFilesOutput$NoData[i]=1
    
    
    Errors[i, "FileName"]=NewFilesOutput$Name[i]
    Errors[i,"SampleLength"]="No Data"
    Errors[i, "Problem"]="No Data Uploaded" 
    
  }
  
  else if(NewFilesOutput$size[i] >2700000 & nchar(as.character(NewFilesOutput$Name[i]))>46){
    
    NewFilesOutput$NoData[i]=1
    
    Errors[i, "FileName"]=NewFilesOutput$Name[i]
    Errors[i,"SampleLength"]="WeirdFileFormat"
    Errors[i, "Problem"]="BadMonitor"}
   
  else {} 
  
}

    ErrorTime=subset(Errors, !(is.na(SampleLength)))
    
    write.table(ErrorTime, file = paste(paste(errorpath,TodayDate,sep=""),".csv",sep=""),append=T, col.names=F)


NewFiles5=subset(NewFilesOutput, NoData==0)

NewFiles=as.vector(NewFiles5$Name)

Errors=data.frame(FileName=as.character(),
                  shutdown_reason=as.numeric(),
                  StartBatteryCharge=as.numeric(),
                  EndBatteryCharge=as.numeric(),
                  StartBatteryVoltage=as.numeric(),
                  EndBatteryVoltage=as.numeric(),
                  SampledVolumeHeader=as.numeric(),
                  SampledRuntimeHeader= as.numeric(),
                  AvgFlow= as.numeric(),
                  TotalVolume=as.numeric(),
                  FlowRate=as.numeric(),
                  SampleLength=as.numeric(),
                  Problem=as.character(), stringsAsFactors = F)



if (length(NewFiles)!=0) {
  
  for (i in 1:length(NewFiles)){

    
    ShortName=NA
    
    #FirmwareVersion=ifelse(substr(NewFiles[i],58,60)=="UTC", as.numeric(substr(as.character(read.table(NewFiles[i], header=F, row.names=NULL, quote = "",sep=",",skip=1, fill=T,na.string=c("","null","NaN"))[1,2]), 14,16)),
    #                       as.numeric(substr(as.character(read.table(NewFiles[i], header=F,quote = "",sep=",", fill=T,na.string=c("","null","NaN"))[1,1]),18,19)) )
    
   FirmwareVersion=ifelse(as.character(read.csv(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[5,1])=="SAMPLE IDENTIFICATION",104,
                          ifelse(substr(NewFiles[i],58,60)=="UTC" | substr(NewFiles[i],59,61)=="UTC", 
                          as.numeric(substr(as.character(read.csv(NewFiles[i], header=F, row.names=NULL, quote = "",sep=",",skip=1, fill=T,na.string=c("","null","NaN"))[1,2]), 14,16)),
                           as.numeric(substr(as.character(read.csv(NewFiles[i], header=F,quote = "",sep=",", fill=T,na.string=c("","null","NaN"))[1,1]),18,19)) ))
    
  if(FirmwareVersion>=15 & FirmwareVersion<90) {
      
      shutdown_reason=NA
      StartBatteryCharge= NA
      StartBatteryVoltage=NA
      EndBatteryCharge=NA
      
      EndBatteryVoltage=NA
      SampledVolumeHeader=NA
      SampledRuntimeHeader=NA
      AvgFlow= NA
      
      #(0=no 1=yes)
      GPSEnabled=NA
    
      CheckFileHeader=read.table(NewFiles[i], header=F,quote = "",sep=",", fill=T,na.string=c("","null","NaN"))[1,c(1:5)]
      CheckOne=substr(CheckFileHeader$V1,18,19)
      CheckOne=as.numeric(as.character(CheckOne))
    
        if (CheckOne==84) {
      
          Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=7,na.string=c("","null","NaN"))[,c(1:19)] }
      
        else { CheckFile=read.table(NewFiles[i], header=F,quote = "",sep=",",skip=4,na.string=c("","null","NaN"))[c(1:5),]
  
                if (CheckFile$V1[1] ==as.character(as.factor("YYMMDDHHMMSS"))) {
      
                  Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=5,na.string=c("","null","NaN"))[,c(1:19)]
                    }
    
                else if (CheckFile$V1[1] ==as.character(as.factor("timestr")))
      
                {Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=4,na.string=c("","null","NaN"))[,c(1:19)]}
 
                else {
                  WrongFirmware=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=4,na.string=c("","null","NaN"))[,c(1:19)]
                  write.table(WrongFirmware, file = paste(paste(paste0(errorpath,'/WrongFirmware',NewFiles[i],sep=""),".csv",sep=""),append=T, col.names=F)
                  } 
    
        }
  }
    
  
    
    ####Flow Problems
    TotalVolumeSampled=Test$sampledVol[nrow(Test)]
    #TotalVolumeSampled=Test[nrow(Test),15]
    #should be close to 1440
  
   
    TotalVol= ifelse( abs(1440-TotalVolumeSampled) < 500, "Correct", "Total Volume Sampled Error") 
    
 
    #insert number for file length
    ShortName=substr(NewFiles[i],64,119)
    #ShortName=substr(NewFiles[1], 65,123)
    }

    if (TotalVol!="Correct" & !(is.na(ShortName))) {
      
          #if correct, then send to another folder
          #for now using folder on Mac
      
          #file.copy(from=NewFiles[i], to=paste(paste("/Users/mshupler/Dropbox/UBC Research/PURE-AIR/Server/ErrorFiles-FlowRate",ShortName,sep="/"),"txt",sep="."))  
      
          #Readin=read.csv(NewFiles[i],sep = "\t" , header = T,skip=4, na.strings=c("","NA"))
          
          
          file.copy(from=NewFiles[i], to=paste(paste("C:/Research/Remote/Uploads/Files/ErrorFiles-TotalVolume",ShortName,sep="/"),"txt",sep="."))
          #file.copy(from=NewFiles[1], to=paste("/Users/mshupler/Dropbox/UBC Research/PURE-AIR/Server/ErrorFiles",ShortName[1],sep="/" ))
      
            #insert file path on server to place Error file in
              #file.copy(from=NewFiles[i], to=paste(paste("C:/Research/Remote/Uploads/ErrorFiles",ShortName,sep="/"),"txt",sep="."))
      
    
          Errors[i, "FileName"]=ShortName
          Errors[i, "shutdown_reason"]=shutdown_reason
          # Errors[1, "FileName"]=ShortName
          Errors[i, "StartBatteryCharge"]=StartBatteryCharge
          Errors[i, "EndBatteryCharge"]=EndBatteryCharge
          Errors[i, "StartBatteryVoltage"]=StartBatteryVoltage
          Errors[i, "EndBatteryVoltage"]=EndBatteryVoltage
          Errors[i, "SampledVolumeHeader"]=SampledVolumeHeader
          Errors[i, "SampledRuntimeHeader"]=SampledRuntimeHeader
          Errors[i, "AvgFlow"]=AvgFlow
          
          Errors[i,"TotalVolume"]=TotalVolumeSampled
          Errors[i, "Problem"]=TotalVol
          #Errors[1,"TotalVolume"]=TotalVol
      
          ErrorComplete=subset(Errors, !(is.na(ShortName)))
          
          j=j+1
      
          write.table(Errors[i,], file = paste(paste("C:/Research/Remote/Uploads/Files/ErrorFiles/ErrorFiles-",TodayDate,sep=""),".csv",sep=""),append=T, col.names=F)
          
          
          }

    
  }
  
  #2nd loop
    

    for (i in 1:length(NewFiles)){
     
      
      Errors=data.frame(FileName=as.character(),
                        shutdown_reason=as.numeric(),
                        StartBatteryCharge=as.numeric(),
                        EndBatteryCharge=as.numeric(),
                        StartBatteryVoltage=as.numeric(),
                        EndBatteryVoltage=as.numeric(),
                        SampledVolumeHeader=as.numeric(),
                        SampledRuntimeHeader= as.numeric(),
                        AvgFlow= as.numeric(),
                        TotalVolume=as.numeric(),
                        FlowRate=as.numeric(),
                        SampleLength=as.numeric(),
                        Problem=as.character(), stringsAsFactors = F)
      
      ShortName=NA
      
      #FirmwareVersion=ifelse(substr(NewFiles[i],58,60)=="UTC", as.numeric(substr(as.character(read.table(NewFiles[i], header=F, row.names=NULL, quote = "",sep=",",skip=1, fill=T,na.string=c("","null","NaN"))[1,2]), 14,16)),
      #as.numeric(substr(as.character(read.table(NewFiles[i], header=F,quote = "",sep=",", fill=T,na.string=c("","null","NaN"))[1,1]),18,19)) )
      
      if(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[5,1])=="SAMPLE IDENTIFICATION") {i=i+1}
      
      FirmwareVersion=ifelse(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[5,1])=="SAMPLE IDENTIFICATION",104,
                             ifelse(substr(NewFiles[i],58,60)=="UTC" | substr(NewFiles[i],59,61)=="UTC", 
                                    as.numeric(substr(as.character(read.table(NewFiles[i], header=F, row.names=NULL, quote = "",sep=",",skip=1, fill=T,na.string=c("","null","NaN"))[1,2]), 14,16)),
                                    as.numeric(substr(as.character(read.table(NewFiles[i], header=F,quote = "",sep=",", fill=T,na.string=c("","null","NaN"))[1,1]),18,19)) ))

      
      
      if(FirmwareVersion>90 & FirmwareVersion<100) {i=i+1}
      
      if(FirmwareVersion==104){
        
        shutdown_reason=as.numeric(substr(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[32,2]),1,1))
        
        StartBatteryCharge= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[26,2]),1,3))
        
        StartBatteryVoltage=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[27,2]),1,3))
        
        EndBatteryCharge=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[30,2]),1,3))
        
        
        EndBatteryVoltage=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[31,2]),1,3))
        
        SampledVolumeHeader= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[33,2]),1,9))
        
        SampledRuntimeHeader= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[35,2]),1,8))
        
        AvgFlow= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[36,2]),1,4))
        
        
        
        #(0=no 1=yes)
        GPSEnabled=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[18,2]),1,1))
        
        
        #QCSampledVolume  
        
        #Test=read.table("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt", quote="", header = T, row.names=NULL, sep=",", skip=42, na.strings=c("","NA"),fill=T)[,c(1:24)]
        
        
        Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=59,na.string=c("","null","NaN"),fill=T)[,c(1:20)]
        
       
        #substr("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt",71,92)
        
        ShortName=substr(NewFiles[i],62,92)
        
        
        
      }
      
      #FirmwareVersion=10 actually is 100, just read only 3 characters
      if(FirmwareVersion<14){
        
        #as.numeric(substr(as.character(read.table("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt", quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[31,2]),1,8))
        #as.numeric(substr(as.character(read.table("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt", quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[23,2]),1,3))
        
        if(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[5,1])=="CALIBRATION COEFFICIENTS") {i=i+1}
        
        
        shutdown_reason=as.numeric(substr(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[23,2]),1,1))
        
        StartBatteryCharge= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[24,2]),1,3))
        
        StartBatteryVoltage=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[25,2]),1,3))
        
        EndBatteryCharge=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[26,2]),1,3))
        
        
        EndBatteryVoltage=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[27,2]),1,3))
        
        SampledVolume= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[28,2]),1,9))
        
        SampledRuntime= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[29,2]),1,8))
        
        AvgFlow= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[31,2]),1,4))
        
        
        
        #(0=no 1=yes)
        GPSEnabled=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[15,2]),1,1))
        
        
        #QCSampledVolume  
        
    
       
        Test=read.table(NewFiles[i], header=F,quote = "",sep=",",skip=43,na.string=c("","null","NaN"),fill=T)[,c(1:19)]
        
        Test<-Test[Test$V4<5,]
       
      }
      
      
      else if(FirmwareVersion>=14 & FirmwareVersion<90) {
        
        shutdown_reason=NA
        StartBatteryCharge= NA
        StartBatteryVoltage=NA
        EndBatteryCharge=NA
        
        EndBatteryVoltage=NA
        SampledVolumeHeader=NA
        SampledRuntimeHeader=NA
        AvgFlow= NA
        
        #(0=no 1=yes)
        GPSEnabled=NA
      
      CheckFileHeader=read.table(NewFiles[i], header=F,quote = "",sep=",", fill=T,na.string=c("","null","NaN"))[1,c(1:5)]
      
      
      CheckOne=substr(CheckFileHeader$V1,18,19)
      CheckOne=as.numeric(as.character(CheckOne))
      
      
      if (CheckOne>=84) {
        
        #need to change UPASfiles to NewFiles
        
        Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=7,na.string=c("","null","NaN"),colClasses = c(timestr='character'),fill=T)[,c(1:19)]
      
      Test<-Test[Test$volflow<5,]
      
      }
      
      else {CheckFile=read.table(NewFiles[i], header=F,quote = "",sep=",",skip=4,na.string=c("","null","NaN"))[c(1:5),] 
      
      #CheckFile=read.table(NewFiles[33], header=F,quote = "",sep=",",skip=4,na.string=c("","null","NaN"))[c(1:5),]
      
      
      if (CheckFile$V1[1] ==as.character(as.factor("YYMMDDHHMMSS"))) {
        
        
        #Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=4,na.string=c("","null","NaN"))
        Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=5,na.string=c("","null","NaN"),colClasses = c(timestr='character'),fill=T)[,c(1:19)]
        
      
        Test<-Test[Test$volflow<5,]
        
        #Test=read.table(NewFiles[38], header=T,quote = "",sep=",",skip=5,na.string=c("","null","NaN"))[,c(1:19)]
        
        
      } else if (CheckFile$V1[1] ==as.character(as.factor("timestr")))
        
      {Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=4,na.string=c("","null","NaN"),colClasses = c(timestr='character'),fill=T)[,c(1:19)]
      
      Test<-Test[Test$volflow<5,]
      
      }
      
      #Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=4)
      
      }
    
  
      
      
      ShortName=substr(NewFiles[i],64,119)
      
      #left off here. Need to edit loops 2 & 3
      #  Test$RateCheck=NULL
       # for (i in (1:(nrow(Test)-1))){
                #Test$RateCheck[i]=if(abs(Test$volflow[i]-1) > 0.5 & abs(Test$volflow[i+1]-1) >0.5) "Flow Rate Error" else NA
                #for testing purposes
        #        Test$RateCheck[i]=if(abs(Test$volflow[i]-1) > 0.5 & abs(Test$volflow[i+1]-1) >0.5) "Flow Rate Error" else NA
                #Test$RateCheck[i]=if(abs(Test$FlowRate[i]-1) > 0.5 & abs(Test$FlowRate[i+1]-1) >0.5) "Flow Rate Error" else NA
     #   }
        
     #   RateError=subset(Test, !(is.na(RateCheck)))
        
      }
    
      
      
       
    
    #  if (nrow(RateError)>1 & !(is.na(ShortName))) {
     # if (RateError1$RateCheck[1]=="Flow Rate Error") {
      
        #RateError1=RateError[1,]
          
        #file.copy(from=NewFiles[i], to=paste(paste("/Users/mshupler/Dropbox/UBC Research/PURE-AIR/Server/ErrorFiles-FlowRate",ShortName,sep="/"),"txt",sep="."))  
    #    write.table(RateError, file=paste(paste("C:/Research/Remote/Uploads/Files/ErrorFiles-FlowRate",ShortName,sep="/"),"txt",sep="."), col.names=T,quote=F,sep=',')
        
      
                
                #RateError1=RateError[!duplicated(RateError$)]  
     
              
        Errors[i, "FileName"]=ShortName
        Errors[i, "shutdown_reason"]=shutdown_reason
        # Errors[1, "FileName"]=ShortName
        Errors[i, "StartBatteryCharge"]=StartBatteryCharge
        Errors[i, "EndBatteryCharge"]=EndBatteryCharge
        Errors[i, "StartBatteryVoltage"]=StartBatteryVoltage
        Errors[i, "EndBatteryVoltage"]=EndBatteryVoltage
        Errors[i, "SampledVolumeHeader"]=SampledVolumeHeader
        Errors[i, "SampledRuntimeHeader"]=SampledRuntimeHeader
        Errors[i, "AvgFlow"]=AvgFlow
        
      #  Errors[i,"FlowRate"]=RateError1$volflow
        Errors[i, "Problem"]="FlowRate"
        #Errors[1,"TotalVolume"]=TotalVol
      
        #ErrorComplete2=subset(Errors, !(is.na(FlowRate)))


                k=k+1
                
    write.table(Errors[i,], file = paste(paste("C:/Research/Remote/Uploads/Files/ErrorFiles/ErrorFiles-",TodayDate,sep=""),".csv",sep=""),append=T, col.names=F)

 }
  
  #3rd loop


  #Length of Sample
  for (i in 1:length(NewFiles)){
    
 
    Errors=data.frame(FileName=as.character(),
                      shutdown_reason=as.numeric(),
                      StartBatteryCharge=as.numeric(),
                      EndBatteryCharge=as.numeric(),
                      StartBatteryVoltage=as.numeric(),
                      EndBatteryVoltage=as.numeric(),
                      SampledVolumeHeader=as.numeric(),
                      SampledRuntimeHeader= as.numeric(),
                      AvgFlow= as.numeric(),
                      TotalVolume=as.numeric(),
                      FlowRate=as.numeric(),
                      SampleLength=as.numeric(),
                      Problem=as.character(), stringsAsFactors = F)
    
    ShortName=NA
    
    #FirmwareVersion=ifelse(substr(NewFiles[i],58,60)=="UTC", as.numeric(substr(as.character(read.table(NewFiles[i], header=F, row.names=NULL, quote = "",sep=",",skip=1, fill=T,na.string=c("","null","NaN"))[1,2]), 14,16)),
    #                       as.numeric(substr(as.character(read.table(NewFiles[i], header=F,quote = "",sep=",", fill=T,na.string=c("","null","NaN"))[1,1]),18,19)) )
    
    
    if(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[5,1])=="SAMPLE IDENTIFICATION") {i=i+1}
    
    FirmwareVersion=ifelse(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[5,1])=="SAMPLE IDENTIFICATION",104,
                           ifelse(substr(NewFiles[i],58,60)=="UTC" | substr(NewFiles[i],59,61)=="UTC", 
                                  as.numeric(substr(as.character(read.table(NewFiles[i], header=F, row.names=NULL, quote = "",sep=",",skip=1, fill=T,na.string=c("","null","NaN"))[1,2]), 14,16)),
                                  as.numeric(substr(as.character(read.table(NewFiles[i], header=F,quote = "",sep=",", fill=T,na.string=c("","null","NaN"))[1,1]),18,19)) ))
    
    if(FirmwareVersion>90 & FirmwareVersion<100) {i=i+1}
    
    if(FirmwareVersion==104){
      
      shutdown_reason=as.numeric(substr(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[32,2]),1,1))
      
      StartBatteryCharge= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[26,2]),1,3))
      
      StartBatteryVoltage=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[27,2]),1,3))
      
      EndBatteryCharge=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[30,2]),1,3))
      
      
      EndBatteryVoltage=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[31,2]),1,3))
      
      SampledVolumeHeader= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[33,2]),1,9))
      
      SampledRuntimeHeader= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[35,2]),1,8))
      
      AvgFlow= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[36,2]),1,4))
      
      
      
      #(0=no 1=yes)
      GPSEnabled=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[18,2]),1,1))
      
      
      #QCSampledVolume  
      
      #Test=read.table("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt", quote="", header = T, row.names=NULL, sep=",", skip=42, na.strings=c("","NA"),fill=T)[,c(1:24)]
      
      
      Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=59,na.string=c("","null","NaN"))[,c(1:20)]
      
      Test$Time=substr(Test$DateTimeUTC,12,19)
      
      Test$Date=substr(Test$DateTimeUTC,1,10)
      
      Test$FullDate=as.POSIXct(strptime(paste(Test$Date, Test$Time, sep=" "), format="%Y-%m-%d %H:%M:%S"))
      
      TimeDiff=difftime(Test$FullDate[nrow(Test)], Test$FullDate[1],units="hours")
      
      #substr("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt",71,92)
      
      ShortName=substr(NewFiles[i],62,92)
      
      
      
    }
    
    #FirmwareVersion=10 actually is 100, just read only 3 characters
    if(FirmwareVersion<14){
      
      #as.numeric(substr(as.character(read.table("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt", quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[31,2]),1,8))
      #as.numeric(substr(as.character(read.table("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt", quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[23,2]),1,3))
      
      if(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[5,1])=="CALIBRATION COEFFICIENTS") {i=i+1}
     
      
      shutdown_reason=as.numeric(substr(as.character(read.table(NewFiles[i], header=F,quote = "",row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[23,2]),1,1))
      
      StartBatteryCharge= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[24,2]),1,3))
      
      StartBatteryVoltage=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[25,2]),1,3))
      
      EndBatteryCharge=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[26,2]),1,3))
      
      
      EndBatteryVoltage=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[27,2]),1,3))
      
      SampledVolumeHeader= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[28,2]),1,9))
      
      SampledRuntimeHeader= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[29,2]),1,8))
      
      AvgFlow= as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[31,2]),1,4))
      
      #(0=no 1=yes)
      GPSEnabled=as.numeric(substr(as.character(read.table(NewFiles[i], quote="", header = F, row.names=NULL, sep=",", skip=1, na.strings=c("","NA"),fill=T)[15,2]),1,1))
      
      
      #QCSampledVolume  
      
      #Test=read.table("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt", quote="", header = T, row.names=NULL, sep=",", skip=42, na.strings=c("","NA"),fill=T)[,c(1:24)]
      
      
     # Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=42,na.string=c("","null","NaN"),fill=T)[,c(1:19)]
      
      Test=read.csv(NewFiles[i], header=F,quote = "",sep=",",skip=43,na.string=c("","null","NaN"),fill=T)[,c(1:19)]
      names(Test)=read.csv(NewFiles[i], header=F,quote = "",sep=",",skip=42,na.string=c("","null","NaN"),fill=T,stringsAsFactors = F)[1,c(1:19)]
      
      
      Test<-Test[Test$VolFlow<5,]
      
      #Test$timestr=as.factor(as.character(as.numeric(Test$timestr)))    
      
      Test$Time=substr(Test$UTCDateTime,12,19)
      
      Test$Date=substr(Test$UTCDateTime,1,10)
      
      Test$FullDate=as.POSIXct(strptime(paste(Test$Date, Test$Time, sep=" "), format="%Y-%m-%d %H:%M:%S"))
      
      TimeDiff=difftime(Test$FullDate[nrow(Test)], Test$FullDate[1],units="hours")
      
    
      #substr("C:/Research/Remote/ThreeSampleFiles/PS0360_LOG_2018-04-06T15_57_08UTC_rev100testfile_____111.txt",71,92)
      
      ShortName=substr(NewFiles[i],62,92)
      
    }
    
    
    
    else if(FirmwareVersion>=15 & FirmwareVersion<90) { 
      
      shutdown_reason=NA
      StartBatteryCharge= NA
      StartBatteryVoltage=NA
      EndBatteryCharge=NA
      
      EndBatteryVoltage=NA
      SampledVolumeHeader=NA
      SampledRuntimeHeader=NA
      AvgFlow= NA
      
      #(0=no 1=yes)
      GPSEnabled=NA
    
    
    CheckFileHeader=read.table(NewFiles[i], header=F,quote = "",sep=",", fill=T,na.string=c("","null","NaN"))[1,c(1:5)]
    
    
    CheckOne=substr(CheckFileHeader$V1,18,19)
    CheckOne=as.numeric(as.character(CheckOne))
    
    
    if (CheckOne>=84) {
      
      #need to change UPASfiles to NewFiles
      
      Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=7,na.string=c("","null","NaN"),colClasses = c(timestr='character'),fill=T)[,c(1:19)]
    
    Test<-Test[Test$volflow<5,]}
    
    else {CheckFile=read.table(NewFiles[i], header=F,quote = "",sep=",",skip=4,na.string=c("","null","NaN"))[c(1:5),]
    
    #CheckFile=read.table(NewFiles[33], header=F,quote = "",sep=",",skip=4,na.string=c("","null","NaN"))[c(1:5),]
    
    
    if (CheckFile$V1[1] ==as.character(as.factor("YYMMDDHHMMSS"))) {
      
      
      #Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=4,na.string=c("","null","NaN"))
      Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=5,na.string=c("","null","NaN"),colClasses = c(timestr='character'),fill=T)[,c(1:19)]
      
      Test<-Test[Test$volflow<5,]
      
      #Test=read.table(NewFiles[38], header=T,quote = "",sep=",",skip=5,na.string=c("","null","NaN"))[,c(1:19)]
      
      
    } else if (CheckFile$V1[1] ==as.character(as.factor("timestr")))
      
    {Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=4,na.string=c("","null","NaN"),colClasses = c(timestr='character'),fill=T)[,c(1:19)]
    
    Test<-Test[Test$volflow<5,]
    
    }
    
    #Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=4)
    
    }
    
    
    #Test=read.table(NewFiles[i], header=T,quote = "",sep=",",skip=4)
  

    ShortName=substr(NewFiles[i],64,119)
    #ShortName=substr(NewFiles[33],64,119)
    
    
    
    Test$Minsec=paste(substr(Test$timestr,9,10),substr(Test$timestr,11,12),sep=':')
    Test$Time=paste(substr(Test$timestr,7,8),Test$Minsec,sep=":")
    
    Test$Date=paste(paste(paste("20",substr(Test$timestr,5,6),sep=""),substr(Test$timestr,3,4),sep="/"),substr(Test$timestr,1,2),sep="/")
    
    Test$FullDate=as.POSIXct(strptime(paste(Test$Date, Test$Time, sep=" "), format="%Y/%m/%d %H:%M:%S"))
    
    TimeDiff=difftime(Test$FullDate[nrow(Test)], Test$FullDate[1],units="hours")
    
    
    }
     
    #TotalTime=difftime(End2, Start2,units='hours')
    #TimeFile=cbind(Shortname[i],"Time Error", TotalTime)
    
    if (TimeDiff < 47 & !(is.na(ShortName))){
    
      file.copy(from=NewFiles[i], to=paste(paste("C:/Research/Remote/Uploads/Files/ErrorFiles-SampleTime",ShortName,sep="/"),"txt",sep="."))  
      
      
      Errors[i, "FileName"]=ShortName
      Errors[i, "shutdown_reason"]=shutdown_reason
      # Errors[1, "FileName"]=ShortName
      Errors[i, "StartBatteryCharge"]=StartBatteryCharge
      Errors[i, "EndBatteryCharge"]=EndBatteryCharge
      Errors[i, "StartBatteryVoltage"]=StartBatteryVoltage
      Errors[i, "EndBatteryVoltage"]=EndBatteryVoltage
      Errors[i, "SampledVolumeHeader"]=SampledVolumeHeader
      Errors[i, "SampledRuntimeHeader"]=SampledRuntimeHeader
      Errors[i, "AvgFlow"]=AvgFlow
      
      Errors[i,"SampleLength"]=TimeDiff
      Errors[i, "Problem"]="RunTime"
      
      #Errors[1,"TotalVolume"]=TotalVol
    
    #ErrorTime=subset(Errors, !(is.na(SampleLength)))
    
    l=l+1
    
     write.table(Errors[i,], file = paste(paste("C:/Research/Remote/Uploads/Files/ErrorFiles/ErrorFiles-",TodayDate,sep=""),".csv",sep=""),append=T, col.names=F) 
     
     }
   
    
  }
  
}
  
  
saveRDS(AlreadyChecked, file="C:/Research/Remote/Uploads/Files/AlreadyChecked.rds")

CountProblems=paste("Dear all, The code ran at", Sys.time(), 
                    "You have", Good, "runs with no errors and",
                    Bad, "runs with at least one error")
  
#send in an email
send_mail_function(CountProblems)

AllErrors=read.table(paste0(errorpath,TodayDate,".csv"), header=F, quote ="\"", sep=" ", na.string=c("","null","NaN"),stringsAsFactors = F)


names(AllErrors)=c("V1","V2")

names(AllErrors)=c("RowNumber",
      "MatchNames")

AllErrors2=AllErrors[!duplicated(AllErrors$MatchNames),]

for (i in (1:nrow(AllErrors2))){
  AllErrors2$MatchNames[i]=if(nchar(AllErrors2$MatchNames[i])>30 & substr(AllErrors2$MatchNames[i],58,60)=="UTC") substr(AllErrors2$MatchNames[i],62,92) else if
                                      (nchar(AllErrors2$MatchNames[i])>30 & substr(AllErrors2$MatchNames[i],58,60)!="UTC") substr(AllErrors2$MatchNames[i],64,nchar(AllErrors2$MatchNames[i])) else AllErrors2$MatchNames[i]
}


UploadedFiles2=NewFiles5[,c(1,2)]
names(UploadedFiles2)[1]="FullName"
UploadedFiles2$FullName=as.character(as.factor(UploadedFiles2$FullName))

for (i in (1:nrow(UploadedFiles2))){
UploadedFiles2$MatchNames[i]=ifelse(substr(UploadedFiles2$FullName[i],58,60)=="UTC", substr(UploadedFiles2$FullName[i],62,92),
                                 substr(UploadedFiles2$FullName[i],64,nchar(UploadedFiles2$FullName[i])) )
}


CorrectFiles=merge(UploadedFiles2,AllErrors2,by="MatchNames",all.x=T)

for (i in 1:nrow(CorrectFiles)){
  file.copy(from=CorrectFiles$FullName[i],to=paste0(correctfilepath,CorrectFiles2$FileName[i]))
  
}

