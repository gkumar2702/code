
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

system('gsutil cp gs://aes-analytics-0001-curated/Outage_Restoration/Model_object/model_AdaptiveLASSO_2020_07_22_22_10_59.RDS /root/')
##Read the Required Forecasted weather File
Weather_data <- read.df(paste0("gs://aes-datahub-0001-raw/Weather/weather_source/USA/Indianapolis/",year_month_1,"/forecast_data/",today,"/weathersource_daily_",tomorrow,".csv"),source = "csv", header="true",inferschema="true")
Weather_data <- SparkR :: collect(Weather_data)
Weather_data <- na.omit(Weather_data)

Weather_data1 <- read.df(paste0("gs://aes-datahub-0001-raw/Weather/weather_source/USA/Indianapolis/",year_month_2,"/forecast_data/",today,"/weathersource_daily_",day_after,".csv"),source = "csv", header="true",inferschema="true")
Weather_data1 <- SparkR :: collect(Weather_data1)
Weather_data1 <- na.omit(Weather_data1)

##Reading the RDS 
model<-readRDS('/root/model_AdaptiveLASSO_2020_07_22_22_10_59.RDS')

model.function <- function(Weather_data) {
  
##Storing the Data
df <- Weather_data

##Storing the time stamp
a <- unique(df$timestamp)

##Dropping unneccessary columns
df <- df[, !(colnames(df) %in% c("V1","latitude",
                                 "longitude","Location",
                                 "timestampInit","precipProb",
                                 "snowfallProb"))]

##Taking min,max & avg of weather variables
df <- df %>%
  group_by(timestamp) %>%
  summarise(cldCvrAvg = mean(cldCvrAvg),
            cldCvrMax = max(cldCvrMax),
            cldCvrMin = min(cldCvrMin),
            
            dewPtAvg = mean(dewPtAvg),
            dewPtMax = max(dewPtMax),
            dewPtMin = min(dewPtMin),
            
            feelsLikeAvg = mean(feelsLikeAvg),
            feelsLikeMax = max(feelsLikeMax),
            feelsLikeMin = min(feelsLikeMin),
            
            heatIndexAvg = mean(heatIndexAvg),
            heatIndexMax = max(heatIndexMax),
            heatIndexMin = min(heatIndexMin),
            
            mslPresAvg = mean(mslPresAvg),
            mslPresMax = max(mslPresMax),
            mslPresMin = min(mslPresMin),
            
            precip = max(precip),
            
            radSolarAvg = mean(radSolarAvg),
            radSolarMax = max(radSolarMax),
            radSolarMin = min(radSolarMin),
            radSolarTot = max(radSolarTot),
            
            relHumAvg = mean(relHumAvg),
            relHumMax = max(relHumMax),
            relHumMin = min(relHumMin),
            
            sfcPresAvg = mean(sfcPresAvg),
            sfcPresMax = max(sfcPresMax),
            sfcPresMin = min(sfcPresMin),
            
            snowfall = max(snowfall),
            
            spcHumAvg = mean(spcHumAvg),
            spcHumMax = max(spcHumMax),
            spcHumMin = min(spcHumMin),
            
            tempAvg = mean(tempAvg),
            tempMax = max(tempMax),
            tempMin = min(tempMin),
            
            wetBulbAvg = mean(wetBulbAvg),
            wetBulbMax = max(wetBulbMax),
            wetBulbMin = min(wetBulbMin),
            
            windChillAvg = mean(windChillAvg),
            windChillMax = max(windChillMax),
            windChillMin = min(windChillMin),
            
            windDir100mAvg = mean(windDir100mAvg),
            windDir80mAvg = max(windDir80mAvg),
            windDirAvg = max(windDirAvg),
            
            windSpd100mAvg = mean(windSpd100mAvg),
            windSpd100mMax = max(windSpd100mMax),
            windSpd100mMin = min(windSpd100mMin),
            
            windSpd80mAvg = mean(windSpd80mAvg),
            windSpd80mMax = max(windSpd80mMax),
            windSpd80mMin = min(windSpd80mMin),
            
            windSpdAvg = mean(windSpdAvg),
            windSpdMax = max(windSpdMax),
            windSpdMin = min(windSpdMin)
            )



#Keeping the required Columns as per feature selection
keeps <- c("cldCvrMax", "mslPresMax" , "mslPresMin", "precip", "radSolarAvg", "radSolarMax",
           "relHumAvg", "relHumMax", "sfcPresMax", "spcHumMax", "windDir100mAvg", "windSpd100mAvg",
           "windSpd100mMax", "windSpd80mMax")
df <- df[keeps]

##Predicting for Storm_Duration
LASSO <- model$predict(model$model,df)
LASSO<-exp(LASSO)    #taking the exponential as predicted result is in log value

#creating column to store output in df
df$predicted_DURATION<-LASSO

#creating column to store the timestamp
df$Date <- a

#Final df
df
}

##Calling  model.function
weather <- model.function(Weather_data)
weather_nextday <- model.function(Weather_data1)

#merging the data into one dataframe
m1 <- merge(weather_nextday, weather, all = TRUE)

#Keeping the required Columns as per feature selection
keeps <- c("Date", "predicted_DURATION" )
m1 <- m1[keeps]

#Writing to GCS
write.csv(m1,paste0('/root/Duration_Prediction','.csv',sep=""),row.names=FALSE)
root_file<-paste0(' /root/Duration_Prediction','.csv',sep="")
output_folder<-paste0('gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Deliverables/Outage_Duration/',year_month,'/',today,'/')
system(paste0("gsutil cp ",root_file," ",output_folder))
output_live<-paste0('gs://us-east4-composer-0001-40ca8a74-bucket/data/Outage_restoration/IPL/STORM_DURATION/')
system(paste0("gsutil cp ",root_file," ",output_live))