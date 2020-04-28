library(shiny)
library(dplyr)
#ibrary(lubridate)

#library(countrycode)
library(DT)
library(scales)
library(JLutils)
library(ggplot2)
library(plotly)
source("map.R")


shinyServer(function(input, output) {

  df = read.csv("data/one_month_data.csv")

  
  

  data = read.csv("data/data_tab.csv", stringsAsFactors = FALSE)
  data = data %>% 
    filter(event_country != '')
  
  
  data$device_type_name = ifelse(data$event_device_type == "1", "Mobile/Tablet",
                                 ifelse(data$event_device_type == "2",  "Personal Computer",
                                        ifelse(data$event_device_type == "3", "Connected TV",
                                               ifelse(data$event_device_type == "4" , "Phone",
                                                      ifelse(data$event_device_type == "5", "Tablet",
                                                             ifelse(data$event_device_type == "6", "Connected Device",
                                                                    ifelse(data$event_device_type == "7", "Set Top Box",
                                                                           ifelse(data$event_device_type == "unknown", "Unknown","Unknown"))))))))
  
  
  
  data$country_name = countrycode(data$event_country, "iso3c", "un.name.en")
  
  data$day <- factor(data$day, levels = c("lundi", "mardi", "mercredi","jeudi", "vendredi", "samedi", "dimanche"))
  
  
  clicks = data %>% 
    filter(X..Event_type1....click.. == 1)
  

  
  ### NEW DATASET###
  total_clicks = sum(df$click)
  

  test = df %>% 
    dplyr::group_by(Country_name,
                    country_code,
                    Timestamp_hour) %>% 
    dplyr::summarise(total_click = sum(click),
                     total_win = dplyr::n(),
                     taux = round(total_click/total_clicks,3)) %>% 
    filter(total_click >0)
  
  

  output$plot <- renderPlotly({
    if(input$s2 == "All" ){
      to_use = test
    }else{
      to_use = test %>% 
        filter(country_code == input$s2) 
    }
    
   ggplotly(ggplot(to_use, aes_string(x = to_use$Timestamp_hour  , y =to_use$taux ))+
      geom_line(aes(color = to_use$country_code)) +
      labs(y=paste("Proportion des clics"), x= "Heure", color ="Pays") +
      scale_y_continuous(labels = percent)) 
  })
  

  
  

  output$plot5 <- renderPlotly({
    if(input$s2 == "All" ){
      d_plot5 = df %>% 
        dplyr::group_by(country_code) %>% 
        dplyr::summarise(total = dplyr::n()) %>% 
        filter(total>50)
    }else{
      d_plot5 = filter(df, country_code %in% input$s2 ) %>% 
        dplyr::group_by(country_code) %>% 
        dplyr::summarise(total = dplyr::n())
    }
    ggplotly(ggplot(d_plot5, aes(reorder(country_code, -total), total)) +
      geom_bar(stat= "identity", fill = '#8193e6') +
      labs(x = "Pays", y = "Nombre de win"))
      
  })
  
  
  

  output$plot2 <- renderPlot({
    if(input$s2 == "All" ){
      dd = data
    }else{
      dd = filter(data, event_country %in% input$s2 )
    }
   ggplot(dd, aes(as.factor(X..Event_type1....click..), fill = event_os)) +
      geom_bar(position= "fill") +
      stat_fill_labels() +
      labs(x = "Clique", y ="Pourcentage de clicks", fill = "Type d'OS") +
      scale_y_continuous(labels = percent)
  })
  
  
  
  
  output$plot3 <- renderPlotly({
    if(input$s2 == "All" ){
      dd= df
    }else{
      dd = filter(df, country_code %in% input$s2 )
    }
    ggplotly(ggplot(dd, aes(as.factor(click), fill = device_type_name)) +
      geom_bar(position= "fill") +
      stat_fill_labels() +
      labs( x = "Clique", y ="Pourcentage de click", fill = "Type d'appareil") +
      scale_y_continuous(labels = percent))
      
  })
  
  
  
  jours = df %>% 
    dplyr::group_by(Country_name,
                    country_code,
                    day) %>% 
    dplyr::summarise(total_click = sum(click),
                     total_win = dplyr::n(),
                     taux = round(total_click/total_clicks,4) *100) %>% 
    filter(total_click>0)
  
  
  
  output$plot6 <- renderPlotly({
    if(input$s2 == "All"){
      data_jour = jours
    }else{
      data_jour = jours %>% 
        filter(country_code == input$s2)
    }
    ggplotly(ggplot(data_jour, aes(x=day, y=taux, fill = country_code)) +
      geom_bar(stat='identity')+
      labs(y ="Proportion des clics", x = "Jours de la semaine") +
      scale_y_continuous(labels = percent))
  })
  
  

  
  
  
 

          
  
  

  #Axe Creative
  
  
  
  sites_all = clicks %>% 
    group_by(event_app_site_name) %>% 
    dplyr::summarise(total =n()) %>% 
    arrange(desc(total))
  
  sites = clicks %>% 
    group_by(event_app_site_name,event_country) %>% 
    dplyr::summarise(total = n()) %>% 
    arrange(desc(total))
  
  
  
  output$table <-  DT::renderDataTable({
    if(input$s4 == "All" ){
      table <- sites_all %>% 
        head(input$slider1)
    }else{
      table = sites %>% 
        filter(event_country == input$s4) %>% 
        head(input$slider1)
    }

    datatable(table, rownames = FALSE)
  })
  
  
  device_make = data %>% 
    filter(X..Event_type1....click.. == 1) %>% 
    group_by(event_country, event_device_make) %>% 
    dplyr::summarise(total = n())%>%
    arrange(desc(total))
  
  
  device_all = data %>% 
    filter(X..Event_type1....click.. == 1) %>% 
    group_by( event_device_make) %>% 
    dplyr::summarise(total = n())%>%
    arrange(desc(total))

  
  output$table2 <-  DT::renderDataTable({
    if(input$s4 == "All" ){
      t <- device_all %>% 
        head(input$slider1)
    }else{
      t= device_make %>% 
        filter(event_country == input$s4) %>% 
        head(input$slider1)
    }
    
    datatable(t, rownames = FALSE)
  })
  
  
  output$plot4 <- renderPlot({
    if(input$s4 == "All"){
      creative = df
    }else{
      creative = df %>% 
        filter(country_code == input$s4)
    }
    
    
    ggplot(creative, aes(x=creative_size))+
        geom_bar(position='identity', fill = "steelblue") +
         labs(x = "creative size ", y ="Nombre de cliques")
  
  })
  
  click_prix = data %>% 
    filter(X..Event_type1....click.. == 1) %>% 
    group_by(win_price_interval, event_country) %>% 
    dplyr::summarise(total_click = n())
  
  
  win_prix = data %>% 
    group_by(win_price_interval, event_country) %>% 
    dplyr::summarise(total_win = n())
  
  
  
  join_prix= inner_join(click_prix, win_prix , by = c("win_price_interval" = "win_price_interval" , "event_country" = "event_country"))
  
  join_prix$pourcentage = join_prix$total_click/join_prix$total_win
  
  
  

  
  output$plot7 <- renderPlot({
    
    if(input$s4 == "All"){
      prix = join_prix
    }else{
      prix = join_prix %>% 
        filter(event_country == input$s4)
    }
    
    
    ggplot(prix, aes(x=win_price_interval, y=pourcentage, fill = event_country)) +
      labs(x = "prix de l'enchère", y = "Pourcentage de cliques", fill = "Pays") +
      geom_bar(stat='identity')+
      scale_y_continuous(labels = percent)
  
  })
  
  
  
  output$map_click  <- renderLeaflet({
    click_map
  })

  # 
  # output$map_win <- renderLeaflet({
  #   win
  # })
  # 
  
  
  
  
  
})
