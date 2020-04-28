library(dplyr)
library(leaflet)
library(rgdal)
library(RColorBrewer)
library(countrycode)
library(viridis)





#df = read.csv("data/oneWeekfullCleanedData.csv")

df_map = read.csv("data/one_month_data.csv")

# df$country_code = countrycode(df$Country_name, 
#                                     origin = 'country.name',
#                                     destination = 'iso3c')
# 
# 
# 
# 
# 
# df$device_type_name = ifelse(df$device_type == "1", "Mobile/Tablet",
#                              ifelse(df$device_type == "2",  "Personal Computer",
#                                     ifelse(df$device_type == "3", "Connected TV",
#                                            ifelse(df$device_type == "4" , "Phone",
#                                                   ifelse(df$device_type == "5", "Tablet",
#                                                          ifelse(df$device_type == "6", "Connected Device",
#                                                                 ifelse(df$device_type == "7", "Set Top Box",
#                                                                        ifelse(df$device_type == "unknown", "Unknown","Unknown"))))))))
# 
# 
# df$day = ifelse(df$Timestamp_DayOfWeek == 1, "Lundi",
#                 ifelse(df$Timestamp_DayOfWeek == 2, "Mardi",
#                        ifelse(df$Timestamp_DayOfWeek == 3, "Mercredi",
#                               ifelse(df$Timestamp_DayOfWeek == 4, "Jeudi",
#                                      ifelse(df$Timestamp_DayOfWeek == 5, "Vendredi",
#                                             ifelse(df$Timestamp_DayOfWeek == 6, "Samedi",
#                                                    ifelse(df$Timestamp_DayOfWeek == 7, "Dimanche", "Erreur")))))))
# 
# 
# df$day <- factor(df$day, levels = c("Lundi", "Mardi", "Mercredi","Jeudi", "Vendredi", "Samedi", "Dimanche"))
# write.csv(df, 'one_month_data.csv')




total_clicks  = sum(df_map$click)

nb_click = df_map %>% 
  dplyr::group_by(Country_name,
                   country_code) %>% 
  dplyr::summarise(total_click = sum(click),
                   total_win = dplyr::n(),
                   taux = round(total_click/total_clicks,3) *100) %>% 
  filter(total_click>0)


# 
# 
# nb_win = df %>% 
#   dplyr::group_by(country_code,
#            Country_name) %>% 
#   dplyr::summarise(total_win = dplyr::n())%>% 
#   filter(total_win>0)


# Read this shape file with the rgdal library. 

world_spdf <- readOGR( 
  dsn= paste0(getwd(),"/data/world_border/") , 
  layer="TM_WORLD_BORDERS_SIMPL-0.3",
  verbose=FALSE
)

# Clean the data object

world_spdf@data$POP2005[ which(world_spdf@data$POP2005 == 0)] = NA
world_spdf@data$POP2005 <- as.numeric(as.character(world_spdf@data$POP2005)) / 1000000 %>% round(2)



world_spdf <- merge(world_spdf,nb_click ,by.x="ISO3", by.y = "country_code", all= TRUE) 
#world_spdf <- merge(world_spdf,nb_win ,by.x="ISO3", by.y = "country_code", all= TRUE) 




bins_click <- c(1,10,50,100, 500, max(nb_click$total_click))

palette_clicks <- colorBin(palette = "Reds",
                           domain=world_spdf@data$total_click,
                           na.color="transparent",
                           bins=bins_click)



text_click <- paste(
  "Pays: ", world_spdf@data$NAME,"<br/>", 
  "Nombre de clics: ", world_spdf@data$total_click, "<br/>", 
  "Nombre de win: ", world_spdf@data$total_win, "<br/>",
  "Taux de clics: ", world_spdf@data$taux, "%", "<br/>",
  sep="") %>%
  lapply(htmltools::HTML)



# Basic choropleth with leaflet?

click_map <- leaflet(world_spdf) %>% 
  addTiles()  %>% 
  setView( lat=10, lng=0 , zoom=2) %>%
  addPolygons( 
    fillColor = ~palette_clicks(total_click), 
    stroke=TRUE, 
    fillOpacity = 0.9, 
    color="white", 
    weight=0.3,
    label = text_click,
    labelOptions = labelOptions( 
      style = list("font-weight" = "normal", padding = "3px 8px"), 
      textsize = "13px", 
      direction = "auto"
    )
  ) %>%
  addLegend( pal=palette_clicks,
             values=~total_click,
             opacity=0.9,
             title = "Nombre de clics",
             position = "bottomleft" )





# 
# bins_win <- c(1,10,50,100, 500,10000, max(nb_win$total_win))
# 
# palette_wins <- colorBin(palette = "plasma",
#                            domain=world_spdf@data$total_win,
#                            na.color="transparent",
#                            bins=bins_win)
#                          
# 
# 
# 
# text_win <- paste(
#   "Pays: ", world_spdf@data$NAME,"<br/>", 
#   "Nombre de win: ", round(world_spdf@data$total_win, 2), "<br/>", 
#   sep="") %>%
#   lapply(htmltools::HTML)
# 
# 
# 
# # Basic choropleth with leaflet?
# 
# win <- leaflet(world_spdf) %>% 
#   addTiles()  %>% 
#   setView( lat=10, lng=0 , zoom=2) %>%
#   addPolygons( 
#     fillColor = ~palette_wins(total_win), 
#     stroke=TRUE, 
#     fillOpacity = 0.9, 
#     color="white", 
#     weight=0.3,
#     label = text_win,
#     labelOptions = labelOptions( 
#       style = list("font-weight" = "normal", padding = "3px 8px"), 
#       textsize = "13px", 
#       direction = "auto"
#     )
#   ) %>%
#   addLegend( pal=palette_wins,
#              values=~total_win,
#              opacity=0.9,
#              title = "Nombre de win",
#              position = "bottomleft" )
# 
# win 
# 
# 
# ggplotly(ggplot(df, aes(df$country_code)) +
#   geom_bar(position= "identity", fill = '#8193e6') +
#   labs(x = "Pays", y = "Nombre de win")+
#     scale_x_discrete(limits = df$country_code))


# win = df %>% 
#   dplyr::group_by(country_code) %>% 
#   dplyr::summarise(total = dplyr::n())
# 
# 
# ggplot(win, aes(x = country_code, y= total)) +
#   geom_bar(stat= "identity", fill = '#8193e6') +
#   labs(x = "Pays", y = "Nombre de win")
# 
# 
# 
# 
# jours = df %>% 
#   dplyr::group_by(Country_name,
#                   country_code,
#                   day) %>% 
#   dplyr::summarise(total_click = sum(click),
#                    total_win = dplyr::n(),
#                    taux = round(total_click/total_clicks,4) *100) %>% 
#   filter(total_click>0)
# 
# 
# click = df %>% 
#   filter(click ==1)
# 
# ggplot(df, aes(as.factor(click), fill = device_type_name)) +
#   geom_bar(position= "fill") +
#   stat_fill_labels() +
#   labs(x = "Clique", y ="Pourcentage de clicks", fill = "Type d'OS") +
#   scale_y_continuous(labels = percent)

  





# 
# device_all = df_map %>% 
#   filter(click == 1) %>% 
#   group_by( event_device_make) %>% 
#   dplyr::summarise(total = n())%>%
#   arrange(desc(total))



# ggplot(df_map, aes(x=creative_size))+
#   geom_bar(position='identity', fill = "steelblue") +
#   labs(x = "creative size ", y ="Nombre de cliques")

