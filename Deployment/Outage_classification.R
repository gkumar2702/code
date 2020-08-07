library('SparkR')
#system('gsutil cp gs://aes-datahub-0002-curated/Outage_Restoration/Model_object/model_Ensemble_2020-05-20-21-11-20.RDS /root/')
library('caret')
library('ranger')
library('xgboost')
library('dplyr')
library('tidyverse')
library('dummies')
sparkR.session()

raw_data <- read.df(paste0("gs://aes-datahub-0002-curated/Outage_Restoration/Staging/IPL_Live_Master_Dataset.csv"), source = "csv", header="true",inferschema="true")
raw_data_df <- SparkR :: collect(raw_data)
raw_data_df<-na.omit(raw_data_df)
model<-readRDS('/root/model_Ensemble_2020-05-20-21-11-20.RDS')

#model<-readRDS('gs://aes-datahub-0002-curated/Outage_Restoration/Model_object/model_Ensemble_2020-05-20-21-11-20.RDS')

dummyfied <- dummy.data.frame(raw_data_df, names = c("clusters","DNI_EQUIP_TYPE","CITY_NAM","Marker_Location","ICON"),sep="_")
dummyfied2 <- dummy.data.frame(dummyfied, names = c("DAY_FLAG","SUBSTATION_CAUSE_FLG","NO_OUTAGE_CAUSE_FLG","DNI_EQUIP_TYPE_PAR_SWITCH","DNI_EQUIP_TYPE_TSWITCH","DNI_EQUIP_TYPE_USWITCH","CITY_NAM_AVON","CITY_NAM_SOUTHPORT","Marker_Location_Marker8","ICON_cloudy","ICON_rain","ICON_snow","ICON_wind","DNI_EQUIP_TYPE_FPO"),sep="_")

if(!"clusters_Cluster3.0" %in% colnames(dummyfied2))
{
  dummyfied2$clusters_Cluster3.0<-0
}

if(!"DAY_FLAG_FALSE" %in% colnames(dummyfied2))
{
  dummyfied2$DAY_FLAG_FALSE<-0
}

if(!"SUBSTATION_CAUSE_FLG_TRUE" %in% colnames(dummyfied2))
{
  dummyfied2$SUBSTATION_CAUSE_FLG_TRUE<-0
}

if(!"NO_OUTAGE_CAUSE_FLG_TRUE" %in% colnames(dummyfied2))
{
  dummyfied2$NO_OUTAGE_CAUSE_FLG_TRUE<-0
}

if(!"DNI_EQUIP_TYPE_FPO_1" %in% colnames(dummyfied2))
{
  dummyfied2$DNI_EQUIP_TYPE_FPO_1<-0
}

if(!"DNI_EQUIP_TYPE_PAR_SWITCH_1" %in% colnames(dummyfied2))
{
  dummyfied2$DNI_EQUIP_TYPE_PAR_SWITCH_1<-0
}

if(!"DNI_EQUIP_TYPE_TSWITCH_1" %in% colnames(dummyfied2))
{
  dummyfied2$DNI_EQUIP_TYPE_TSWITCH_1<-0
}

if(!"DNI_EQUIP_TYPE_USWITCH_1" %in% colnames(dummyfied2))
{
  dummyfied2$DNI_EQUIP_TYPE_USWITCH_1<-0
}

if(!"CITY_NAM_AVON_1" %in% colnames(dummyfied2))
{
  dummyfied2$CITY_NAM_AVON_1<-0
}

if(!"CITY_NAM_SOUTHPORT_1" %in% colnames(dummyfied2))
{
  dummyfied2$CITY_NAM_SOUTHPORT_1<-0
}

if(!"Marker_Location_Marker8_1" %in% colnames(dummyfied2))
{
  dummyfied2$Marker_Location_Marker8_1<-0
}

if(!"ICON_cloudy_1" %in% colnames(dummyfied2))
{
  dummyfied2$ICON_cloudy_1<-0
}

if(!"ICON_rain_0" %in% colnames(dummyfied2))
{
  dummyfied2$ICON_rain_0<-0
}

if(!"ICON_snow_1" %in% colnames(dummyfied2))
{
  dummyfied2$ICON_snow_1<-0
}

if(!"ICON_wind_1" %in% colnames(dummyfied2))
{
  dummyfied2$ICON_wind_1<-0
}

if(!"LIVE_OUTAGE" %in% colnames(dummyfied2))
{
  dummyfied2$LIVE_OUTAGE<-0
}

if(!"RANK_SUBSEQUENT_OUTAGES" %in% colnames(dummyfied2))
{
  dummyfied2$RANK_SUBSEQUENT_OUTAGES<-0
}

finaldata <- dummyfied2 %>% select(clusters_Cluster3.0,DAY_FLAG_FALSE,SUBSTATION_CAUSE_FLG_TRUE,NO_OUTAGE_CAUSE_FLG_TRUE,DNI_EQUIP_TYPE_FPO_1,DNI_EQUIP_TYPE_PAR_SWITCH_1,DNI_EQUIP_TYPE_TSWITCH_1,DNI_EQUIP_TYPE_USWITCH_1,CITY_NAM_AVON_1,CITY_NAM_SOUTHPORT_1,Marker_Location_Marker8_1,ICON_cloudy_1,ICON_rain_0,ICON_snow_1,ICON_wind_1,CALL_QTY,DOWNSTREAM_CUST_QTY,CUST_QTY,LIVE_OUTAGE,DEWPOINT,HUMIDITY,PRECIPINTENSITY,PRECIPINTENSITYMAX,PRESSURE,TEMPERATUREMAX,TEMPERATUREMIN,VISIBILITY,WINDBEARING,WINDGUST,WINDSPEED,RANK_SUBSEQUENT_MAJ_OTG_ID)
finaldata<-finaldata%>%rename(DAY_FLAG_0=DAY_FLAG_FALSE)
finaldata<-finaldata%>%rename(SUBSTATION_CAUSE_FLG_1=SUBSTATION_CAUSE_FLG_TRUE)
finaldata<-finaldata%>%rename(NO_OUTAGE_CAUSE_FLG_1=NO_OUTAGE_CAUSE_FLG_TRUE)
finaldata<-finaldata%>%rename(PRECIPINTENSITY.1=PRECIPINTENSITYMAX)
finaldata<-finaldata%>%rename(RANK_SUBSEQUENT_OUTAGES=RANK_SUBSEQUENT_MAJ_OTG_ID)
finaldata2<-finaldata%>%rename(clusters_Cluster3=clusters_Cluster3.0)

model$modelSuite$XGB$predFunc <- function(model, test, factorLabels, thresh=0.5){
    names = colnames(test)
    if(nrow(test) == 1) {
      test = sapply(test, as.numeric) %>% dplyr::bind_rows() %>% as.data.frame()
    } else {
      test = data.frame(sapply(test, as.numeric))
    }
    colnames(test) <- names
    pred = predict(model, as.matrix(test), probability = T)
    p = cut(pred, breaks = c(0, as.numeric(thresh), 1), labels = rev(factorLabels),
            include.lowest = T)
    return(p)
  }


model$modelSuite$RF$predFunc<-function(model, test, factorLabels, thresh=0.5, testCat=NULL, xSelected=NULL){
  if(!is.null(testCat))
  {
    test <- getCategoricalData(test, testCat, xSelected)$data
  }
  pred = ranger::predictions(stats::predict(model, data.frame(test), type="response"))
  pred = pred[,make.names(factorLabels)]
  if(nrow(test) == 1) {
    p = cut(pred[2], breaks=c(0,as.numeric(thresh),1), labels=factorLabels, include.lowest = T)
  } else {
    p = cut(pred[,2], breaks=c(0,as.numeric(thresh),1), labels=factorLabels, include.lowest = T)
  }
 
  return(p)
}

ensemble<-model$predFunc(model$modelSuite,finaldata2,model$type,model$weights)
finaldata2$label<-ensemble
raw_data_df$label<-ensemble

dump_time<-format(Sys.time(),"%Y%m%d%H%M")
write.csv(raw_data_df,paste0('/root/IPL_Input_Live_Master_Dataset',dump_time,'.csv',sep=""),row.names=FALSE)
write.csv(raw_data_df,paste0('/root/IPL_Input_Live_Master_Dataset.csv'),row.names=FALSE)
root_file_dump<-paste0(' /root/IPL_Input_Live_Master_Dataset',dump_time,'.csv',sep="")
root_file<-paste0(' /root/IPL_Input_Live_Master_Dataset.csv')
output_folder<-paste0(' gs://aes-datahub-0002-curated/Outage_Restoration/Staging/')
output_folder_dump<-paste0(" gs://aes-datahub-0002-curated/Outage_Restoration/Back_up/",sep="")
system(paste0("gsutil cp",root_file,output_folder,sep=" "))
system(paste0("gsutil cp",root_file_dump,output_folder_dump,sep=" "))