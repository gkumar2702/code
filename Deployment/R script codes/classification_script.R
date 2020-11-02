
##Loading the required packages 
library('SparkR')
sparkR.session()
library(dplyr)
library(magrittr)
library(Matrix)
library(glmnet)

#loading all required dates to variables
today <- format(Sys.time(),"%Y-%m-%d")
tomorrow <- format(as.Date(Sys.time(),format="%Y%m%d")+1,"%Y%m%d")
day_after <- format(as.Date(Sys.time(),format="%Y%m%d")+2,"%Y%m%d")
year_month <- format(Sys.time(),"%Y-%m")
year_month_1 <- format(as.Date(Sys.time(),format="%Y%m%d")+1,"%Y-%m")
year_month_2 <- format(as.Date(Sys.time(),format="%Y%m%d")+2,"%Y-%m")

system('gsutil cp gs://aes-analytics-0001-curated/Outage_Restoration/Model_object/model_RF_2020-08-20-15-25-54.RDS /root/')
##Read the Required Forecasted weather File
Weather_data <- read.df(paste0("gs://aes-datahub-0001-raw/Weather/weather_source/USA/Indianapolis/",year_month_1,"/forecast_data/",today,"/weathersource_daily_",tomorrow,".csv"),source = "csv", header="true",inferschema="true")
Weather_data <- SparkR :: collect(Weather_data)
Weather_data <- na.omit(Weather_data)

Weather_data1 <- read.df(paste0("gs://aes-datahub-0001-raw/Weather/weather_source/USA/Indianapolis/",year_month_2,"/forecast_data/",today,"/weathersource_daily_",day_after,".csv"),source = "csv", header="true",inferschema="true")
Weather_data1 <- SparkR :: collect(Weather_data1)
Weather_data1 <- na.omit(Weather_data1)

##Adding the forecast type
Weather_data$forecast_type<-"One day ahead"
Weather_data1$forecast_type<-"Two days ahead"

#merging the data into one dataframe
weather_data_raw <- merge(Weather_data, Weather_data1, all = TRUE)

##Reading the RDS 
model<-readRDS("/root/model_RF_2020-08-20-15-25-54.RDS")

##Dropping unneccessary columns
weather_data_raw <- weather_data_raw[, !(colnames(weather_data_raw) %in% c("V1",
                                 "timestampInit","precipProb",
                                 "snowfallProb"))]

##Predicting for Storm_Duration
cluster<-model$predict(model$model,weather_data_raw[,model$features],model$factorLabels)

#creating column to store output in df
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

##selecting necessary columns
m1<-weather_data_raw%>%select(timestamp,forecast_type,latitude,longitude,Location,profile,prob_Cluster1,prob_Cluster2,prob_Cluster3,prob_Cluster4,prob_Cluster5,prob_Cluster6)

m1<-m1%>%rename(Date=timestamp)
maxn <- function(n) function(x) order(x, decreasing = TRUE)[n]
m1$profile_2<-apply(m1[,7:12], 1, maxn(2))
m1$prob_2<-apply(m1[,7:12], 1, function(x)x[maxn(2)(x)])

m1<-m1%>%mutate(profile_desc2=case_when(
  .$profile_2==1~"Hot Days with Sudden Rain",
  .$profile_2==2~"Strong Breeze with Sudden Rain",
  .$profile_2==3~"Thunderstorms",
  .$profile_2==4~"Chilly Day with Chances of Snow",
  .$profile_2==5~"Strong Chilled Breeze with Chances of Snow",
  .$profile_2==6~"Hot Days with Chance of Rain"
))

m1<-m1%>%select(Date,forecast_type,latitude,longitude,Location,profile,prob_Cluster1,prob_Cluster2,prob_Cluster3,prob_Cluster4,prob_Cluster5,prob_Cluster6,profile_desc2,prob_2)
#Writing to GCS
write.csv(m1,paste0('/root/forecast_storm_profiles','.csv',sep=""),row.names=FALSE)
root_file<-paste0(' /root/forecast_storm_profiles','.csv',sep="")
output_folder_live<-paste0('gs://aes-analytics-0001-curated/Outage_Restoration/Live_Data_Curation/Forecast_Storm_Profiles/')
output_folder<-paste0('gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Deliverables/Storm_Profiles/',year_month,'/',today,'/')
system(paste0("gsutil cp ",root_file," ",output_folder))
system(paste0("gsutil cp ",root_file," ",output_folder_live))
