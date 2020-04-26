library(dplyr)
library(neuralnet)
library(xgboost)
library(caret)
library(fastDummies)

df = read.csv("data/oneWeekfullCleanedData.csv")


res = dummy_cols(df, select_columns = c("app_or_site", "bidder_name", "device_type"))


train <- res %>% 
  sample_frac(.75)

test  <- anti_join(res, train, by = 'auction_id')

X_train = train %>% 
  select(-auction_id, -click, -exchange, 
         -app_or_site, -has_gps, - bidder_name, -connection_type, -has_ifa,
         -Device_language, -starts_with('Country'),
         -Continent, -City, -Timestamp, -Device_lg_Equals_country_lg, -fullOsInfo,
         -device_type)

Y_train = train %>% select(click)



X_test = test %>% 
  select(-auction_id, -click, -exchange, 
         -app_or_site, -has_gps, - bidder_name, -connection_type, -has_ifa,
         -Device_language, -starts_with('Country'),
         -Continent, -City, -Timestamp, -Device_lg_Equals_country_lg, -fullOsInfo,
         -device_type)

Y_test = test %>% select(click)

bstSparse <- xgboost(data = as.matrix(X_train),
                     label = as.matrix(Y_train),
                     max.depth = 10,
                     eta = 1,
                     nthread = 2,
                     nrounds = 2,
                     objective = "binary:logistic",
                     verbose = 2)



dtrain <- xgb.DMatrix(data = as.matrix(X_train), label= as.matrix(Y_train))
dtest <- xgb.DMatrix(data = as.matrix(X_test), label= as.matrix(Y_test))
