library('SparkR')
system('gsutil cp gs://aes-datahub-0002-curated/Outage_Restoration/Model_object/model_RF_2020_07_03_20_19_53.RDS /root/')
system('gsutil cp gs://aes-datahub-0002-curated/Outage_Restoration/Model_object/model_RF_2020_06_22_15_24_05.RDS /root/')
library('caret')
library('ranger')
library('xgboost')
library('dplyr')
library('tidyverse')
library('dummies')
library('bigrquery')
sparkR.session()


raw_data_df <- read.df(paste0("gs://aes-datahub-0002-curated/Outage_Restoration/Staging/IPL_Input_Live_Master_Dataset.csv"), source = "csv", header="true",inferschema="true")
raw_data <- SparkR :: collect(raw_data_df)
raw_data<-na.omit(raw_data)

print('Data loaded')

model<-readRDS('/root/model_RF_2020_07_03_20_19_53.RDS')
raw_data1<-raw_data%>%filter(raw_data$label==0)
raw_data1<-na.omit(raw_data1)
cols_needed<-model$colsUsed
cols_needed<-cols_needed[-1]
input<-names(raw_data1)[names(raw_data1) %in% cols_needed]

if("PRIORITY_VAL_2" %in% colnames(raw_data))
{
raw_data<-raw_data%>%rename(PRIORITY_VAL_2.0=PRIORITY_VAL_2)
}
if("PRIORITY_VAL_3" %in% colnames(raw_data))
{
raw_data<-raw_data%>%rename(PRIORITY_VAL_3.0=PRIORITY_VAL_3)
}
if("PRIORITY_VAL_5" %in% colnames(raw_data))
{
raw_data<-raw_data%>%rename(PRIORITY_VAL_5.0=PRIORITY_VAL_5)
}

if((is.data.frame(raw_data1) && nrow(raw_data1)==0))
{
print('There are no less than 24hrs outages')
}

if(!(is.data.frame(raw_data1) && nrow(raw_data1)==0))
{

dummyfied<-dummy.data.frame(raw_data1,names =  c("PRECIPTYPE","clusters","PRIORITY_VAL_3.0","PRIORITY_VAL_2.0","OH_OCCURN_FLG","SWITCH_OCCURN_FLG","WIRE_OCCURN_FLG","PUBLIC_SAFETY_OCCURN_FLG","ST_OCCURN_FLG","TRANSFORMER_OCCURN_FLG","MISCELLANEOUS_CAUSE_FLG","TREE_CAUSE_FLG","TRANSFORMER_CLUE_FLG","EQUIPMENT_CLUE_FLG","WIRE_DOWN_CLUE_FLG","TREE_CLUE_FLG","OPEN_DEVICE_CLUE_FLG","POWER_OUT_CLUE_FLG","PART_LIGHT_CLUE_FLG","POLE_CLUE_FLG","DNI_EQUIP_TYPE","OPEN_DEVICE_CLUE_FLG","Marker_Location"),sep="_")


if(!"Marker_Location_Marker4" %in% colnames(dummyfied))
{
  dummyfied$Marker_Location_Marker4<-0
}

if(!"Marker_Location_Marker8" %in% colnames(dummyfied))
{
  dummyfied$Marker_Location_Marker8<-0
}

if(!"clusters_Cluster7.0" %in% colnames(dummyfied))
{
  dummyfied$clusters_Cluster7.0<-0
}

if(!"PRECIPTYPE_rain" %in% colnames(dummyfied))
{
  dummyfied$PRECIPTYPE_rain<-0
}


if(!"PRIORITY_VAL_3.0_FALSE" %in% colnames(dummyfied))
{
  dummyfied$PRIORITY_VAL_3.0_FALSE<-1
}

if(!"PRIORITY_VAL_2.0_TRUE" %in% colnames(dummyfied))
{
  dummyfied$PRIORITY_VAL_2.0_TRUE<-0
}

if(!"SWITCH_OCCURN_FLG_FALSE" %in% colnames(dummyfied))
{
  dummyfied$SWITCH_OCCURN_FLG_FALSE<-1
}

if(!"WIRE_OCCURN_FLG_FALSE" %in% colnames(dummyfied))
{
  dummyfied$WIRE_OCCURN_FLG_FALSE<-1
}

if(!"SERVICE_OCCURN_FLG_FALSE" %in% colnames(dummyfied))
{
  dummyfied$SERVICE_OCCURN_FLG_FALSE<-1
}

if(!"PUBLIC_SAFETY_OCCURN_FLG_FALSE" %in% colnames(dummyfied))
{
  dummyfied$PUBLIC_SAFETY_OCCURN_FLG_FALSE<-1
}

if(!"MISCELLANEOUS_CAUSE_FLG_FALSE" %in% colnames(dummyfied))
{
  dummyfied$MISCELLANEOUS_CAUSE_FLG_FALSE<-1
}

if(!"TRANSFORMER_CLUE_FLG_FALSE" %in% colnames(dummyfied))
{
  dummyfied$TRANSFORMER_CLUE_FLG_FALSE<-1
}

if(!"DAY_FLAG_TRUE" %in% colnames(dummyfied))
{
  dummyfied$DAY_FLAG_TRUE<-0
}

if(!"OPEN_DEVICE_CLUE_FLG_FALSE" %in% colnames(dummyfied))
{
  dummyfied$OPEN_DEVICE_CLUE_FLG_FALSE<-1
}

if(!"POWER_OUT_CLUE_FLG_TRUE" %in% colnames(dummyfied))
{
  dummyfied$POWER_OUT_CLUE_FLG_TRUE<-0
}

if(!"PART_LIGHT_CLUE_FLG_FALSE" %in% colnames(dummyfied))
{
  dummyfied$PART_LIGHT_CLUE_FLG_FALSE<-1
}

if(!"DNI_EQUIP_TYPE_BPSWITCH" %in% colnames(dummyfied))
{
  dummyfied$DNI_EQUIP_TYPE_BPSWITCH<-0
}

if(!"DNI_EQUIP_TYPE_SSUB" %in% colnames(dummyfied))
{
  dummyfied$DNI_EQUIP_TYPE_SSUB<-0
}

if(!"DNI_EQUIP_TYPE_USWITCH" %in% colnames(dummyfied))
{
  dummyfied$DNI_EQUIP_TYPE_USWITCH<-0
}

if(!"RANK_SUBSEQUENT_MAJ_OTG_ID" %in% colnames(dummyfied))
{
  dummyfied$RANK_SUBSEQUENT_MAJ_OTG_ID<-0
}

finaldata<-dummyfied%>%select(clusters_Cluster7.0,PRECIPTYPE_rain,Marker_Location_Marker4,Marker_Location_Marker8,PRIORITY_VAL_3.0_FALSE,PRIORITY_VAL_2.0_TRUE,SWITCH_OCCURN_FLG_FALSE,WIRE_OCCURN_FLG_FALSE,SERVICE_OCCURN_FLG_FALSE,PUBLIC_SAFETY_OCCURN_FLG_FALSE,
                              MISCELLANEOUS_CAUSE_FLG_FALSE,TRANSFORMER_CLUE_FLG_FALSE,OPEN_DEVICE_CLUE_FLG_FALSE,POWER_OUT_CLUE_FLG_TRUE,
                              PART_LIGHT_CLUE_FLG_FALSE,DAY_FLAG_TRUE,DNI_EQUIP_TYPE_BPSWITCH,DNI_EQUIP_TYPE_SSUB,DNI_EQUIP_TYPE_USWITCH,CALL_QTY,DOWNSTREAM_CUST_QTY,CUST_QTY,DOWNSTREAM_KVA_VAL,KVA_VAL,
                              ZONE,LIVE_OUTAGE,CLOUDCOVER,DEWPOINT,PRECIPINTENSITY,PRECIPINTENSITYMAX,PRESSURE,TEMPERATUREMAX,TEMPERATUREMIN,VISIBILITY,WINDBEARING,WINDGUST,WINDSPEED,Hour_Sin,Hour_Cos,RANK_SUBSEQUENT_MAJ_OTG_ID)
rf<-model$predict(model$model,finaldata)
rf<-exp(rf)
raw_data1$predicted_TTR<-rf
result_final<-raw_data1%>%select(OUTAGE_ID,INCIDENT_ID,STRCTUR_NO,CIRCT_ID,DNI_EQUIP_TYPE,clusters,CREATION_DATETIME,predicted_TTR)
print('Less than 24 hrs outages are given predictions')
}


model2<-readRDS('/root/model_RF_2020_06_22_15_24_05.RDS')
raw_data2<-raw_data%>%filter(raw_data$label==1)
raw_data2<-na.omit(raw_data2)
result_final2<-data.frame()

if((is.data.frame(raw_data2) && nrow(raw_data2)==0))
{
print('There are no more than 24 hrs outages')
}

if(!(is.data.frame(raw_data2) && nrow(raw_data2)==0))
{
  dummyfied2<-dummy.data.frame(raw_data2,names = c("DNI_EQUIP_TYPE","OPEN_DEVICE_CLUE_FLG","clusters","Marker_Location","PRECIPTYPE","POLE_CLUE_FLG","DAY_FLAG","SWITCH_OCCURN_FLG","PRIORITY_VAL_2.0","PRIORITY_VAL_5.0"),sep="_")
  
  if(!"clusters_Cluster2.0" %in% colnames(dummyfied2))
  {
    dummyfied2$clusters_Cluster2.0<-0
  }
  
  if(!"clusters_Cluster3.0" %in% colnames(dummyfied2))
  {
    dummyfied2$clusters_Cluster3.0<-0
  }
  
  if(!"clusters_Cluster4.0" %in% colnames(dummyfied2))
  {
    dummyfied2$clusters_Cluster4.0<-0
  }
  
  if(!"clusters_Cluster5.0" %in% colnames(dummyfied2))
  {
    dummyfied2$clusters_Cluster5.0<-0
  }
  
  if(!"clusters_Cluster6.0" %in% colnames(dummyfied2))
  {
    dummyfied2$clusters_Cluster6.0<-0
  }
  
  if(!"clusters_Cluster7.0" %in% colnames(dummyfied2))
  {
    dummyfied2$clusters_Cluster7.0<-0
  }
  
  if(!"PRECIPTYPE_snow" %in% colnames(dummyfied2))
  {
    dummyfied2$PRECIPTYPE_snow<-0
  }
  
  if(!"Marker_Location_Marker4" %in% colnames(dummyfied2))
  {
    dummyfied2$Marker_Location_Marker4<-0
  }
  
  if(!"POLE_CLUE_FLG_FALSE" %in% colnames(dummyfied2))
  {
    dummyfied2$POLE_CLUE_FLG_FALSE<-0
  }
  
  if(!"DAY_FLAG_TRUE" %in% colnames(dummyfied2))
  {
    dummyfied2$DAY_FLAG_TRUE<-0
  }
  
  if(!"PRIORITY_VAL_2.0_TRUE" %in% colnames(dummyfied2))
  {
    dummyfied2$PRIORITY_VAL_2.0_TRUE<-0
  }
  
  if(!"PRIORITY_VAL_5.0_TRUE" %in% colnames(dummyfied2))
  {
    dummyfied2$PRIORITY_VAL_5.0_TRUE<-0
  }
  
  if(!"SWITCH_OCCURN_FLG_FALSE" %in% colnames(dummyfied2))
  {
    dummyfied2$SWITCH_OCCURN_FLG_FALSE<-0
  }
  
  dummyfied2<-dummyfied2%>%select(clusters_Cluster2.0,clusters_Cluster3.0,clusters_Cluster4.0,clusters_Cluster5.0,clusters_Cluster6.0,clusters_Cluster7.0,PRECIPTYPE_snow,Marker_Location_Marker4,POLE_CLUE_FLG_FALSE,DAY_FLAG_TRUE,CALL_QTY,DOWNSTREAM_CUST_QTY,DOWNSTREAM_KVA_VAL,KVA_VAL,PRIORITY_VAL_2.0_TRUE,PRIORITY_VAL_5.0_TRUE,ZONE,LIVE_OUTAGE,CLOUDCOVER,DEWPOINT,HUMIDITY,PRECIPINTENSITY,PRECIPINTENSITYMAX,PRESSURE,TEMPERATUREMAX,TEMPERATUREMIN,VISIBILITY,WINDBEARING,WINDGUST,Hour_Sin,Hour_Cos,RANK_SUBSEQUENT_MAJ_OTG_ID,SWITCH_OCCURN_FLG_FALSE,WINDSPEED)
  finaldata2<-dummyfied2
  finaldata2<-finaldata2%>%rename(PRIORITY_VAL_2.0=PRIORITY_VAL_2.0_TRUE)
  finaldata2<-finaldata2%>%rename(PRIORITY_VAL_5.0=PRIORITY_VAL_5.0_TRUE)
  rf2<-model2$predict(model2$model,finaldata2)
  rf2<-exp(rf2)
  finaldata2$predicted_TTR<-rf2
  raw_data2$predicted_TTR<-rf2
  result_final2<-raw_data2%>%select(OUTAGE_ID,INCIDENT_ID,STRCTUR_NO,CIRCT_ID,DNI_EQUIP_TYPE,clusters,CREATION_DATETIME,predicted_TTR) 
}

if((is.data.frame(result_final2) && nrow(result_final2)==0))
	{
		result_collated<-result_final
	}else if (nrow(raw_data1)==0&& nrow(raw_data2)!=0)
	{
	result_collated<-result_final2
	}else
	{result_collated<-rbind(result_final,result_final2)
	}
print('Result collation is done')
result_collated<-result_collated%>%mutate(profile=case_when(
  .$clusters=="Cluster1.0"~"Rainfall with Lightning",
  .$clusters=="Cluster2.0"~"Dry and Cloudy",
  .$clusters=="Cluster3.0"~"Snowfall and Ice",
  .$clusters=="Cluster4.0"~"Clear and Dry",
  .$clusters=="Cluster5.0"~"Rainfall with winds",
  .$clusters=="Cluster6.0"~"Drizzle with Thunder",
  .$clusters=="Cluster7.0"~"Drizzle with Heavy winds",
  .$clusters=="Cluster8.0"~"Heavy rains with Thunder"
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

write.csv(result_formatted,paste0("/root/TTR_predictions_",format(Sys.time(),"%Y%m%d"),".csv"),row.names=FALSE)
write.csv(result_formatted,paste0("/root/TTR_predictions_",dump_time,".csv"),row.names=FALSE)
root_file<-paste0("/root/TTR_predictions_",format(Sys.time(),"%Y%m%d"),".csv")
root_file_dump=paste0("/root/TTR_predictions_",dump_time,".csv")
output_file<-paste0("gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/TTR_Predictions/",sep="") 
system(paste0("gsutil cp ",root_file,"  ",output_file,sep=""))
output_file_dump<-paste0("gs://aes-datahub-0002-curated/Outage_Restoration/OMS/Deliverables/ERTs/",format(dump_time_file,"%Y-%m"),"/",format(dump_time_file,"%Y-%m-%d"),"/",sep="")
system(paste0("gsutil cp ",root_file_dump,"  ",output_file_dump,sep=""))

print('Regression complete')
