#Loading the required packages and initiating the spark session
library('SparkR')
sparkR.session()
library(dplyr)
library(magrittr)
library(Matrix)
library(tidyverse)
library(glmnet)

#Read the Required Forecasted weather File
#raw_data <- data.table::fread('C:/Users/kakaraparthi.b/Desktop/AES/Outage/Weekday_Weekend_Pipeline/IPL_Live_Master_Dataset_Weekday_Weekend.csv',
                               #   header = TRUE, stringsAsFactors = TRUE, na.strings = c("NA")) %>% data.frame()

raw_data <- read.df(paste0("gs://aes-datahub-0002-curated/Outage_Restoration/Staging/IPL_Live_Master_Dataset_ws.csv"),source = "csv", header="true",inferschema="true")
raw_data <- SparkR :: collect(raw_data)
raw_data <- na.omit(raw_data)
data1<-raw_data%>%filter(raw_data$weekend_flag==0)
result_final1<-data.frame()
if(!(is.data.frame(data1) && nrow(data1)==0))
{
  #Reading the RDS 
  #model<-readRDS('C:/Users/kakaraparthi.b/Desktop/AES/Outage/Weekday_Weekend_Pipeline/model_RF_2020_08_18_23_01_14.RDS')
  model<-readRDS('/root/model_RF_2020_08_18_23_01_14.RDS')
  Outage_df1 <- data1
  #Adding the required columns
  Outage_df1$PRIORITY_VAL_2.0_TRUE <- ifelse(Outage_df1$PRIORITY_VAL_2 == TRUE, 1, 0)
  Outage_df1$PUBLIC_SAFETY_OCCURN_FLG_FALSE <- ifelse(Outage_df1$PUBLIC_SAFETY_OCCURN_FLG == FALSE, 1,0)
  Outage_df1$OPEN_DEVICE_CLUE_FLG_FALSE <- ifelse(Outage_df1$OPEN_DEVICE_CLUE_FLG == FALSE, 1, 0)
  Outage_df1$POWER_OUT_CLUE_FLG_TRUE <- ifelse(Outage_df1$POWER_OUT_CLUE_FLG == TRUE, 1, 0)
  Outage_df1$POLE_CLUE_FLG_FALSE <- ifelse(Outage_df1$POLE_CLUE_FLG == FALSE, 1, 0)
  Outage_df1["sfcPresRatio"] <- Outage_df1$sfcPresMax/Outage_df1$sfcPresMin
  
  df <- Outage_df1
  
  #Keeping the required Columns as per feature selection
  keeps <- c("PRIORITY_VAL_2.0_TRUE", "PUBLIC_SAFETY_OCCURN_FLG_FALSE" , "OPEN_DEVICE_CLUE_FLG_FALSE",
             "POWER_OUT_CLUE_FLG_TRUE", "POLE_CLUE_FLG_FALSE", "CALL_QTY",
             "DOWNSTREAM_CUST_QTY", "cldCvrAvg", "feelsLikeAvg", "precip", "radSolarAvg",
             "radSolarMax", "radSolarTot", "tempMin", "wetBulbAvg", "windDir100mAvg", "windSpd100mAvg",
             "windSpd80mAvg", "sfcPresRatio", "Outages_in_last_1hr", "Outages_in_last_2hr",
             "Outages_in_last_3hr", "Outages_in_last_4hr", "Outages_in_last_5hr",
             "Outages_in_last_6hr", "Outages_in_last_7hr", "Outages_in_last_8hr",
             "Outages_in_last_9hr", "Outages_in_last_10hr")
  df <- df[keeps]
  
  #Predicting for ETR
  RF <- model$predict(model$model,df)
  RF<- RF^(3)    #taking the exponential as predicted result is in log value
  
  #creating column to store output in df
  data1$predicted_TTR <- RF
  result_final1<-data1%>%select(OUTAGE_ID,INCIDENT_ID,STRCTUR_NO,CIRCT_ID,DNI_EQUIP_TYPE,clusters,CREATION_DATETIME,predicted_TTR)
}

data2<-raw_data%>%filter(raw_data$weekend_flag==1)
Outage_df2 <- data2
result_final2<-data.frame()
if(!(is.data.frame(data2) && nrow(data2)==0))
{
  #Reading the RDS 
  #model2<-readRDS('C:/Users/kakaraparthi.b/Desktop/AES/Outage/Weekday_Weekend_Pipeline/model_RF_2020_08_19_16_14_03weekend.RDS')
  model2<-readRDS('/root/model_RF_2020_08_19_16_14_03weekend.RDS')
  ###Adding the required columns
  Outage_df2$PRIORITY_VAL_2.0_TRUE <- ifelse(Outage_df2$PRIORITY_VAL_2 == TRUE, 1, 0)
  Outage_df2$OPEN_DEVICE_CLUE_FLG_FALSE <- ifelse(Outage_df2$OPEN_DEVICE_CLUE_FLG == FALSE, 1, 0)
  Outage_df2$POWER_OUT_CLUE_FLG_TRUE <- ifelse(Outage_df2$POWER_OUT_CLUE_FLG == TRUE, 1, 0)
  Outage_df2["relHumRatio"] <- Outage_df2$relHumMax/Outage_df2$relHumMin
  Outage_df2["sfcPresRatio"] <- Outage_df2$sfcPresMax/Outage_df2$sfcPresMin
  
  df2 <- Outage_df2
  
  #Keeping the required Columns as per feature selection
  keeps2 <- c("PRIORITY_VAL_2.0_TRUE", "OPEN_DEVICE_CLUE_FLG_FALSE","POWER_OUT_CLUE_FLG_TRUE",
              "CALL_QTY", "DOWNSTREAM_CUST_QTY", "Hour_Sin", "dewPtAvg", "dewPtMax", "dewPtMin",
              "feelsLikeMin", "mslPresMin", "radSolarAvg", "radSolarMax", "radSolarTot", "relHumAvg",
              "relHumMin", "sfcPresMin", "spcHumMax", "spcHumMin", "tempMax", "wetBulbMax",
              "wetBulbMin", "windDir100mAvg", "windDir80mAvg", "windDirAvg", "windSpd100mAvg",
              "windSpd100mMax", "windSpd80mAvg", "windSpd80mMax", "windSpd80mMin", "windSpdAvg",
              "windSpdMax", "windSpdMin", "relHumRatio", "sfcPresRatio",
              "Outages_in_last_1hr", "Outages_in_last_2hr",
              "Outages_in_last_3hr", "Outages_in_last_4hr", "Outages_in_last_5hr",
              "Outages_in_last_6hr", "Outages_in_last_7hr", "Outages_in_last_8hr",
              "Outages_in_last_9hr", "Outages_in_last_10hr")
  df2 <- df2[keeps2]
  
  ##Predicting for ETR
  RF2 <- model2$predict(model2$model,df2)
  RF2<- RF2^(3)    #taking the exponential as predicted result is in log value
  
  #creating column to store output
  data2$predicted_TTR <- RF2
  
  result_final2<-data2%>%select(OUTAGE_ID,INCIDENT_ID,STRCTUR_NO,CIRCT_ID,DNI_EQUIP_TYPE,clusters,CREATION_DATETIME,predicted_TTR)
}
if((is.data.frame(result_final2) && nrow(result_final2)==0))
{
  result_collated<-result_final1
}else{result_collated<-rbind(result_final1,result_final2)}

result_collated<-result_collated%>%mutate(profile=case_when(
  .$clusters=="Cluster1"~"Hot Days with Sudden Rain",
  .$clusters=="Cluster2"~"Strong Breeze with Sudden Rain",
  .$clusters=="Cluster3"~"Thunderstorms",
  .$clusters=="Cluster4"~"Chilly Day with Chances of Snow",
  .$clusters=="Cluster5"~"Strong Chilled Breeze with Chances of Snow",
  .$clusters=="Cluster6"~"Hot Days with Chance of Rain"
))

result_collated$Creation_Time <- as.POSIXct(raw_data$CREATION_DATETIME,format = "%Y-%m-%d %H:%M:%S", tz = "EDT")
result_collated$ETR<-result_collated$Creation_Time+result_collated$predicted_TTR*60+300
result_collated<-result_collated%>%rename(Restoration_Period=predicted_TTR)

result_formatted<-result_collated%>%select(OUTAGE_ID,INCIDENT_ID,STRCTUR_NO,CIRCT_ID,DNI_EQUIP_TYPE,Creation_Time,ETR,Restoration_Period,profile)

result_formatted$Restoration_Period<-format(round(result_formatted$Restoration_Period,0),nsmall=0)

result_formatted$ETR <-as.POSIXlt(round(as.double(result_formatted$ETR)/(10*60))*(10*60),origin=(as.POSIXlt('1970-01-01')))

result_formatted<-result_formatted%>%rename(Estimated_Restoration_Time=ETR,ETR=Restoration_Period,Weather_Profile=profile)

print('Formatting of output done')


time<-format(Sys.time(),tz='America/Indianapolis',usetz=TRUE)

dump_time_edt<-format(Sys.time(),tz='America/Indianapolis',usetz=TRUE)
dump_time_file<-as.POSIXct(dump_time_edt,format="%Y-%m-%d %H:%M:%S")

dump_time<-format(dump_time_file,"%Y%m%d%H%M")

write.csv(result_formatted,paste0("/root/TTR_predictions_ws_",format(Sys.time(),"%Y%m%d"),".csv"),row.names=FALSE)
write.csv(result_formatted,paste0("/root/TTR_predictions_ws__",dump_time,".csv"),row.names=FALSE)
root_file<-paste0("/root/TTR_predictions_ws_",format(Sys.time(),"%Y%m%d"),".csv")
root_file_dump=paste0("/root/TTR_predictions_ws_",dump_time,".csv")
output_file<-paste0("gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/TTR_Predictions/",sep="")
system(paste0("gsutil cp ",root_file,"  ",output_file,sep=""))
output_file_dump<-paste0("gs://aes-datahub-0002-curated/Outage_Restoration/OMS/Deliverables/ERTs/",format(dump_time_file,"%Y-%m"),"/",format(dump_time_file,"%Y-%m-%d"),"/",sep="")
system(paste0("gsutil cp ",root_file_dump,"  ",output_file_dump,sep=""))

write.csv(result_formatted,paste0("/root/ERT_predictions.csv"),row.names=FALSE)
root_file_live<-paste0("/root/ERT_predictions.csv")
output_file_live<-paste0("gs://aes-datahub-0002-curated/Outage_Restoration/OMS/ERT_live/",sep="")
system(paste0("gsutil cp ",root_file_live,"  ",output_file_live,sep=""))
print('Regression complete')
