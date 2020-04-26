library(dplyr)
library(leaflet)
library(rgdal)
library(RColorBrewer)
library(countrycode)
library(viridis)





#df = read.csv("data/oneWeekfullCleanedData.csv")

df = read.csv("data/data_one_month_cleaned_over_sampling.csv")

df$country_code = countrycode(df$Country_name, 
                                    origin = 'country.name',
                                    destination = 'iso3c')



nb_click = df %>% 
  dplyr::group_by(Country_name,
                   country_code) %>% 
  dplyr::summarise(total_click = sum(click),
                   total_win = dplyr::n(),
                   taux = round(total_click/total_win,2) *100) %>% 
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




bins_click <- c(1,25,50,100)

palette_clicks <- colorBin(palette = "Reds",
                           domain=world_spdf@data$taux,
                           na.color="transparent",
                           bins=bins_click)



text_click <- paste(
  "Pays: ", world_spdf@data$NAME,"<br/>", 
  "Nombre de cliques: ", world_spdf@data$total_click, "<br/>", 
  "Nombre de win: ", world_spdf@data$total_win, "<br/>",
  "Taux de cliques: ", world_spdf@data$taux, "%", "<br/>",
  sep="") %>%
  lapply(htmltools::HTML)



# Basic choropleth with leaflet?

click <- leaflet(world_spdf) %>% 
  addTiles()  %>% 
  setView( lat=10, lng=0 , zoom=2) %>%
  addPolygons( 
    fillColor = ~palette_clicks(taux), 
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
             values=~taux,
             opacity=0.9,
             title = "Taux de cliques",
             position = "bottomleft" )


click  



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
