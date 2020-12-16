library('SparkR')
sparkR.session()
library('caret')
library('ranger')
library('xgboost')
library('dplyr')
library('dummies')

system('gsutil cp gs://aes-analytics-0001-curated/Outage_Restoration/Model_object/model_RF_2020-08-20-15-25-54.RDS /root/')
date<-format(Sys.Date(),"%Y%m%d")
input_filename<-paste0("gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Weather_live_IPL/Weather_forecast_daily_TODAY.csv")

weather_data_raw_df <- read.df(input_filename, source = "csv", header="true",inferschema="true")
weather_data_raw <- SparkR :: collect(weather_data_raw_df)

model<-readRDS("/root/model_RF_2020-08-20-15-25-54.RDS")

cluster<-model$predict(model$model,weather_data_raw[,model$features],model$factorLabels)

cluster<-data.frame(cluster)
weather_data_raw$clusters<-cluster$pred
weather_data_raw$prob_Cluster1<-cluster$prob.Cluster1
weather_data_raw$prob_Cluster2<-cluster$prob.Cluster2
weather_data_raw$prob_Cluster3<-cluster$prob.Cluster3
weather_data_raw$prob_Cluster4<-cluster$prob.Cluster4
weather_data_raw$prob_Cluster5<-cluster$prob.Cluster5
weather_data_raw$prob_Cluster6<-cluster$prob.Cluster6

weather_data_raw<-weather_data_raw%>%mutate(profile=case_when(
  .$clusters=="Cluster1"~"Hot Days with Sudden Rain",
  .$clusters=="Cluster2"~"Strong Breeze with Sudden Rain",
  .$clusters=="Cluster3"~"Thunderstorms",
  .$clusters=="Cluster4"~"Chilly Day with Chances of Snow",
  .$clusters=="Cluster5"~"Strong Chilled Breeze with Chances of Snow",
  .$clusters=="Cluster6"~"Hot Days with Chance of Rain"
))

m1<-weather_data_raw%>%select(timestamp,latitude,longitude,Location,profile,prob_Cluster1,prob_Cluster2,prob_Cluster3,prob_Cluster4,prob_Cluster5,prob_Cluster6)

write.csv(weather_data_raw,paste0("/root/weather_data_clusters_ws.csv"),row.names=FALSE)
output_file<-paste0("gs://aes-analytics-0001-curated/Outage_Restoration/Live_Data_Curation/Storm_Profiles/storm_profiles_",date,".csv",sep="")
system(paste0("gsutil cp /root/weather_data_clusters_ws.csv"," ",output_file,sep=""))