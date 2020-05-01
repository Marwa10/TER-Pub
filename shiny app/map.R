library(dplyr)
library(leaflet)
library(rgdal)
library(RColorBrewer)
library(countrycode)
library(viridis)


df_map = read.csv("data/one_month_data.csv")



total_clicks  = sum(df_map$click)

nb_click = df_map %>% 
  dplyr::group_by(Country_name,
                   country_code) %>% 
  dplyr::summarise(total_click = sum(click),
                   total_win = dplyr::n(),
                   taux = round(total_click/total_clicks,3) *100) %>% 
  filter(total_click>0)


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




