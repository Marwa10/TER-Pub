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









df$device_type_name = ifelse(df$device_type == "1", "Mobile/Tablet",
                             ifelse(df$device_type == "2",  "Personal Computer",
                                    ifelse(df$device_type == "3", "Connected TV",
                                           ifelse(df$device_type == "4" , "Phone",
                                                  ifelse(df$device_type == "5", "Tablet",
                                                         ifelse(df$device_type == "6", "Connected Device",
                                                                ifelse(df$device_type == "7", "Set Top Box",
                                                                       ifelse(df$device_type == "unknown", "Unknown","Unknown"))))))))


df$day = ifelse(df$Timestamp_DayOfWeek == 1, "Lundi",
                ifelse(df$Timestamp_DayOfWeek == 2, "Mardi",
                       ifelse(df$Timestamp_DayOfWeek == 3, "Mercredi",
                              ifelse(df$Timestamp_DayOfWeek == 4, "Jeudi",
                                     ifelse(df$Timestamp_DayOfWeek == 5, "Vendredi",
                                            ifelse(df$Timestamp_DayOfWeek == 6, "Samedi",
                                                   ifelse(df$Timestamp_DayOfWeek == 7, "Dimanche", "Erreur")))))))


df$day <- factor(df$day, levels = c("Lundi", "Mardi", "Mercredi","Jeudi", "Vendredi", "Samedi", "Dimanche"))

clicks = df %>% 
  filter(click== 1)



test = df %>% 
  dplyr::group_by(Country_name,
                  country_code,
                  Timestamp_hour) %>% 
  dplyr::summarise(total_click = sum(click),
                   total_win = dplyr::n(),
                   taux = round(total_click/total_clicks,3)) %>% 
  filter(total_click >0)


ggplotly(ggplot(test, aes_string(x = test$Timestamp_hour , y = test$taux))+
           geom_line(aes(color = test$country_code)) +
           labs(y=paste("Proportion des clics"), x= "Heure", color ="Pays") +
           scale_y_continuous(labels = percent))


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
