library('SparkR')
library('caret')
library('ranger')
library('xgboost')
library('dplyr')
library('tidyverse')
library('dummies')
sparkR.session()
system('gsutil cp gs://aes-datahub-0002-curated/Outage_Restoration/Model_object/model_RF_2020-06-17-17-06-30.RDS /root/')
date<-format(Sys.Date(),"%Y%m%d")
month<-format(Sys.Date(),"%Y-%m")
input_filename<-paste0("gs://aes-datahub-0002-raw/Weather/Dark_Sky/USA/Indianapolis/",month,"/actual_Data/darkskyweatherdaily_",date,".csv",sep="")

weather_data_raw_df <- read.df(input_filename, source = "csv", header="true",inferschema="true")
weather_data_raw <- SparkR :: collect(weather_data_raw_df)

model<-readRDS("/root/model_RF_2020-06-17-17-06-30.RDS")
weather_data_raw<-weather_data_raw%>%select(-c(apparentTemperatureMax,apparentTemperatureMin))
weather_data_raw$Location<-sprintf('Marker%i',weather_data_raw$Location)
weather_data_curated<-weather_data_raw%>%select(-Date)
weather_data_curated$icon<-gsub("-",".",weather_data_curated$icon)
dummyfied <- dummy.data.frame(weather_data_curated, names =c("Location"),sep="_")
dummyfied2 <- fastDummies::dummy_cols(dummyfied)
if(!"icon_cloudy" %in% colnames(dummyfied2))
{
  dummyfied2$icon_cloudy<-0
}

if(!"icon_partly.cloudy.day" %in% colnames(dummyfied2))
{
  dummyfied2$icon_partly.cloudy.day<-0
}

if(!"icon_snow" %in% colnames(dummyfied2))
{
  dummyfied2$icon_snow<-0
}

if(!"icon_clear.day" %in% colnames(dummyfied2))
{
  dummyfied2$icon_clear.day<-0
}

if(!"icon_rain" %in% colnames(dummyfied2))
{
  dummyfied2$icon_rain<-0
}

if(!"precipType_snow" %in% colnames(dummyfied2))
{
  dummyfied2$precipType_snow<-0
}
if(!"precipIntensityMax" %in% colnames(dummyfied2))
{
  dummyfied2$precipIntensityMax<-0
}
if("precipType_" %in% colnames(dummyfied2))
{
  dummyfied2<-dummyfied2%>%select(-c("precipType_"))
}
dummyfied2<-dummyfied2%>%select(-c("icon","Location_Marker6","precipType_snow","precipProbability","precipType"))

rf<-model$predict(model$model,dummyfied2,model$factorLabels)
rf2<-gsub("c","C",rf$pred)
rf2<-gsub("Clusters","Cluster",rf2)
rf2<-paste0(rf2,".0")
weather_data_raw$clusters<-rf2
write.csv(weather_data_raw,paste0("/root/weather_data_clusters.csv"),row.names=FALSE)
output_file<-paste0("gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/Storm_Profiles/storm_profiles_",date,".csv",sep="")
system(paste0("gsutil cp /root/weather_*"," ",output_file,sep=""))