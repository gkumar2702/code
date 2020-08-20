library('SparkR')
sparkR.session()

library('caret')
library('ranger')
library('xgboost')
library('dplyr')
library('tidyverse')
library('dummies')

system('gsutil cp gs://aes-analytics-0002-curated/Outage_Restoration/Model_object/model_RF_2020-08-20-15-25-54.RDS /root/')
date<-format(Sys.Date(),"%Y-%m-%d")
date2<-format(Sys.Date(),"%Y%m%d")
month<-format(Sys.Date(),"%Y-%m")
input_filename<-paste0("gs://aes-datahub-0002-raw/Weather/weather_source/USA/Indianapolis/",month,"/forecast_data/",date,"/weathersource_daily_",date2,".csv",sep="")

weather_data_raw_df <- read.df(input_filename, source = "csv", header="true",inferschema="true")
weather_data_raw <- SparkR :: collect(weather_data_raw_df)

model<-readRDS("/root/model_RF_2020-08-20-15-25-54.RDS")


cluster<-model$predict(model$model,weather_data_raw[,model$features],model$factorLabels)

weather_data_raw$clusters<-cluster$pred

write.csv(weather_data_raw,paste0("/root/weather_data_clusters_ws.csv"),row.names=FALSE)
output_file<-paste0("gs://aes-analytics-0002-curated/Outage_Restoration/Live_Data_Curation/Storm_Profiles_ws/storm_profiles_",date2,".csv",sep="")
system(paste0("gsutil cp /root/weather_data_clusters_ws.csv"," ",output_file,sep=""))
