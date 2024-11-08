# loading the required packages 
library('SparkR')
sparkR.session()
library(dplyr)
library(magrittr)
library(Matrix)
library(glmnet)
library(earth)

# loading all required dates to variables
today <- format(Sys.time(),"%Y-%m-%d")
tomorrow <- format(as.Date(Sys.time(),format="%Y%m%d")+1,"%Y%m%d")
day_after <- format(as.Date(Sys.time(),format="%Y%m%d")+2,"%Y%m%d")
year_month <- format(Sys.time(),"%Y-%m")
year_month_1 <- format(as.Date(Sys.time(),format="%Y%m%d")+1,"%Y-%m")
year_month_2 <- format(as.Date(Sys.time(),format="%Y%m%d")+2,"%Y-%m")

# Read the Required Forecasted weather File
Weather_data <- read.df(paste0("gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Weather_live_IPL/Weather_forecast_daily_TOM.csv"),source = "csv", header="true",inferschema="true")
Weather_data <- SparkR :: collect(Weather_data)
Weather_data <- na.omit(Weather_data)

Weather_data1 <- read.df(paste0("gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Weather_live_IPL/Weather_forecast_daily_DAY_A_TOM.csv"),source = "csv", header="true",inferschema="true")
Weather_data1 <- SparkR :: collect(Weather_data1)
Weather_data1 <- na.omit(Weather_data1)

# Reading the RDS 
model<-readRDS('/root/model_MARS_2020_08_14_17_52_29.RDS')

model.function <- function(Weather_data) {
  
  # Storing the Data
  df <- Weather_data
  
  # Storing the time stamp
  a <- unique(df$timestamp)
  
  # Dropping unneccessary columns
  df <- df[, !(colnames(df) %in% c("V1","latitude",
                                   "longitude","Location",
                                   "timestampInit","precipProb",
                                   "snowfallProb"))]
  
  # Taking min,max & avg of weather variables
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
  
  
  
  # Keeping the required Columns as per feature selection
  keeps <- c("dewPtAvg", "feelsLikeAvg" , "feelsLikeMax", "feelsLikeMin", "heatIndexAvg", "heatIndexMin",
             "mslPresMax", "mslPresMin", "precip", "radSolarAvg", "sfcPresMax", "sfcPresMin",
             "spcHumAvg", "windSpd100mAvg","windSpd100mMax","windSpd80mMax","windSpdAvg","windSpdMax")
  df <- df[keeps]
  
  # Predicting for Storm_Duration
  MARS <- model$predict(model$model,df)
  MARS<-(MARS)*(MARS)*(MARS)
  
  # creating column to store output in df
  df$Predicted_Cust_Qty<-MARS
  
  # creating column to store the timestamp
  df$Date <- a
  
  # final df
  df
}

# Calling  model.function
weather <- model.function(Weather_data)
weather_nextday <- model.function(Weather_data1)

# merging the data into one dataframe
m1 <- merge(weather_nextday, weather, all = TRUE)

# Keeping the required Columns as per feature selection
keeps <- c("Date", "Predicted_Cust_Qty" )
m1 <- m1[keeps]
m1$Predicted_Cust_Qty <-round(m1$Predicted_Cust_Qty)

# Residuals for different ranges
resid_0_2500 <- 1363.56
resid_2500_4000 <- 1566.72
resid_4000_7000 <- 3850.02
resid_7000_20000 <- 5592.56
resid_20000_ <- 4387.43

m1$Customers_LL_95 <- ifelse(m1$Predicted_Cust_Qty <= 2500, (m1$Predicted_Cust_Qty-1.96*resid_0_2500),
                                ifelse((m1$Predicted_Cust_Qty > 2500) & (m1$Predicted_Cust_Qty <= 4000), (m1$Predicted_Cust_Qty-1.96*resid_2500_4000),
                                       ifelse((m1$Predicted_Cust_Qty > 4000) & (m1$Predicted_Cust_Qty <= 7000), (m1$Predicted_Cust_Qty-1.96*resid_4000_7000),
                                              ifelse((m1$Predicted_Cust_Qty > 7000) & (m1$Predicted_Cust_Qty <= 20000), (m1$Predicted_Cust_Qty-1.96*resid_7000_20000),
                                                     ifelse((m1$Predicted_Cust_Qty > 20000), (m1$Predicted_Cust_Qty-1.96*resid_20000_), NA)))))

m1$Customers_UL_95 <- ifelse(m1$Predicted_Cust_Qty <= 2500, (m1$Predicted_Cust_Qty+1.96*resid_0_2500),
                                ifelse((m1$Predicted_Cust_Qty > 2500) & (m1$Predicted_Cust_Qty <= 4000), (m1$Predicted_Cust_Qty+1.96*resid_2500_4000),
                                       ifelse((m1$Predicted_Cust_Qty > 4000) & (m1$Predicted_Cust_Qty <= 7000), (m1$Predicted_Cust_Qty+1.96*resid_4000_7000),
                                              ifelse((m1$Predicted_Cust_Qty > 7000) & (m1$Predicted_Cust_Qty <= 20000), (m1$Predicted_Cust_Qty+1.96*resid_7000_20000),
                                                     ifelse((m1$Predicted_Cust_Qty > 20000), (m1$Predicted_Cust_Qty+1.96*resid_20000_), NA)))))

# Converting Negative predictions to 0
m1$Customers_LL_95[m1$Customers_LL_95 < 0] <- 0

# Formatting decimals
m1$Customers_LL_95 <- format(round(m1$Customers_LL_95, 0), nsmall = 0)
m1$Customers_UL_95 <- format(round(m1$Customers_UL_95, 0), nsmall = 0)

m1$Customers_LL_95 <- as.integer(m1$Customers_LL_95)
m1$Customers_UL_95 <- as.integer(m1$Customers_UL_95)

# Writing to GCS
write.csv(m1,paste0('/root/Predicted_Cust_Qty','.csv',sep=""),row.names=FALSE)
root_file<-paste0(' /root/Predicted_Cust_Qty','.csv',sep="")
output_folder<-paste0('gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Deliverables/Outage_Duration/',year_month,'/',today,'/')
system(paste0("gsutil cp ",root_file," ",output_folder))
output_live<-paste0('gs://us-east4-composer-0001-8d07c42c-bucket/data/Outage_restoration/IPL/CUSTOMER_QUANTITY/')
system(paste0("gsutil cp ",root_file," ",output_live))
