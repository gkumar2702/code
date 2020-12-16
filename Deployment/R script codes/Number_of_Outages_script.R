##Loading the required packages 
library('SparkR')
sparkR.session()
library(dplyr)
library(magrittr)
library(Matrix)
library(glmnet)
library(plyr)
library(stats)


#loading all required dates to variables
today <- format(Sys.time(),"%Y-%m-%d")
tomorrow <- format(as.Date(Sys.time(),format="%Y%m%d")+1,"%Y%m%d")
day_after <- format(as.Date(Sys.time(),format="%Y%m%d")+2,"%Y%m%d")
year_month <- format(Sys.time(),"%Y-%m")


##Read the Required Files
Weather_data <- read.df(paste0("gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Deliverables/Outage_Duration/",year_month,"/",today,"/","PCA1.csv"),source = "csv", header="true",inferschema="true")
Weather_data <- SparkR :: collect(Weather_data)
Weather_data <- na.omit(Weather_data)

Weather_data1 <- read.df(paste0("gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Deliverables/Outage_Duration/",year_month,"/",today,"/","PCA2.csv"),source = "csv", header="true",inferschema="true")
Weather_data1 <- SparkR :: collect(Weather_data1)
Weather_data1 <- na.omit(Weather_data1)


##Reading the RDS 
#model_step <-readRDS('C:/Users/kakaraparthi.b/Desktop/AES/Outage/New_Pipeline/model_stepwise_2020_07_21_18_59_49.RDS')
model_step <-readRDS('/root/model_stepwise_2020_07_21_18_59_49.RDS')

model.function <- function(Weather_data) {
  
df <- Weather_data

a <- unique(df$Date)

##Dropping unneccessary columns
df <- df[, !(colnames(df) %in% c("Date"))]

##Running model for prediction
stepwise <- model_step$predict(model_step$model,df)

##taking log as values were scaled before pca
stepwise <-exp(stepwise) 

#creating column to store output in df
df$predicted_Number_of_OUTAGES<-stepwise

#creating column to store the timestamp
df$Date <- a

#Keeping the required Columns 
keeps <- c("Date", "predicted_Number_of_OUTAGES")
df <- df[keeps]

#Final df
df

}


##Calling  model.function
weather <- model.function(Weather_data)
weather_nextday <- model.function(Weather_data1)

#merging the data into one dataframe
m1 <- merge(weather_nextday, weather, all = TRUE)


# Residuals for different ranges
resid_0_25 <- 13.52
resid_25_50 <- 12.07
resid_50_100 <- 15.13
resid_100_250 <- 35.90
resid_250_ <- 172.96

# For 95% confidence intervals
m1$Outages_LL_95 <- ifelse(m1$predicted_Number_of_OUTAGES <= 25, (m1$predicted_Number_of_OUTAGES- 1.96*resid_0_25),
                              ifelse((m1$predicted_Number_of_OUTAGES > 25) & (m1$predicted_Number_of_OUTAGES <= 50), (m1$predicted_Number_of_OUTAGES-1.96*resid_25_50),
                                     ifelse((m1$predicted_Number_of_OUTAGES > 50) & (m1$predicted_Number_of_OUTAGES <= 100), (m1$predicted_Number_of_OUTAGES-1.96*resid_50_100),
                                            ifelse((m1$predicted_Number_of_OUTAGES > 100) & (m1$predicted_Number_of_OUTAGES <= 250), (m1$predicted_Number_of_OUTAGES-1.96*resid_100_250),
                                                   ifelse((m1$predicted_Number_of_OUTAGES > 250), (m1$predicted_Number_of_OUTAGES-1.96*resid_250_), NA)))))

m1$Outages_UL_95 <- ifelse(m1$predicted_Number_of_OUTAGES <= 25, (m1$predicted_Number_of_OUTAGES+1.96*resid_0_25),
                              ifelse((m1$predicted_Number_of_OUTAGES > 25) & (m1$predicted_Number_of_OUTAGES <= 50), (m1$predicted_Number_of_OUTAGES+1.96*resid_25_50),
                                     ifelse((m1$predicted_Number_of_OUTAGES > 50) & (m1$predicted_Number_of_OUTAGES <= 100), (m1$predicted_Number_of_OUTAGES+1.96*resid_50_100),
                                            ifelse((m1$predicted_Number_of_OUTAGES > 100) & (m1$predicted_Number_of_OUTAGES <= 250), (m1$predicted_Number_of_OUTAGES+1.96*resid_100_250),
                                                   ifelse((m1$predicted_Number_of_OUTAGES > 250), (m1$predicted_Number_of_OUTAGES+1.96*resid_250_), NA)))))

# Converting Negative predictions to 0
m1$Outages_LL_95[m1$Outages_LL_95 < 0] <- 0

# Formatting decimals
m1$Outages_LL_95 <- format(round(m1$Outages_LL_95, 0), nsmall = 0)
m1$Outages_UL_95 <- format(round(m1$Outages_UL_95, 0), nsmall = 0)

m1$Outages_LL_95 <- as.integer(m1$Outages_LL_95)
m1$Outages_UL_95 <- as.integer(m1$Outages_UL_95)

#Writing to GCS
write.csv(m1,paste0('/root/Outages_Prediction','.csv',sep=""),row.names=FALSE)
root_file<-paste0(' /root/Outages_Prediction','.csv',sep="")
output_folder<-paste0('gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Deliverables/Outage_Duration/',year_month,'/',today,'/')
system(paste0("gsutil cp ",root_file," ",output_folder))
output_live<-paste0('gs://us-east4-composer-0001-8d07c42c-bucket/data/Outage_restoration/IPL/NUMBER_OF_OUTAGES/')
system(paste0("gsutil cp ",root_file," ",output_live))