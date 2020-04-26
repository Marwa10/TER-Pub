library(shiny)
library(dplyr)
library(lubridate)
library(ggplot2)
library(countrycode)
library(DT)
library(scales)
library(JLutils)
library(leaflet)
source("map.R")


shinyServer(function(input, output) {
  data = read.csv("data/data_tab.csv", stringsAsFactors = FALSE)
  df = read.csv("data/oneWeekfullCleanedData.csv")
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
  

  
  ##Axe User

  
  clique = clicks %>% 
    group_by(hour = hour(X..timestamp....click..), event_country) %>% 
    dplyr::summarise(total_click = n())
  
  
  win = data %>% 
    group_by(hour = hour(X..timestamp....win..), event_country) %>% 
    dplyr::summarise(total_win = n())
  
  
  
  join = inner_join(clique, win , by = c("hour" = "hour", "event_country" = "event_country"))
  
  join$pourcentage = join$total_click/join$total_win
  
  
  
  output$plot <- renderPlot({
    if(input$s2 == "All" ){
      df = join
    }else{
      df = join %>% 
        filter(event_country == input$s2) 
    }

    ggplot(df, aes_string(x = df$hour , y = df$pourcentage))+
      geom_line(aes(color = df$event_country)) +
      labs(y=paste("Proportion des cliques"), x= "Heure", color ="Pays") +
      scale_y_continuous(labels = percent)
  })
  
  
  
  
  
  
  output$plot5 <- renderPlot({
    if(input$s2 == "All" ){
      d_plot5 = data
    }else{
      d_plot5 = filter(data, event_country %in% input$s2 )
    }
    ggplot(d_plot5, aes(event_country)) +
      geom_bar(position= "identity", fill = '#8193e6') +
      labs(x = "Pays", y = "Nombre de win")
      
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
  
  
  
  
  output$plot3 <- renderPlot({
    if(input$s2 == "All" ){
      dd= data
    }else{
      dd = filter(data, event_country %in% input$s2 )
    }
    ggplot(dd, aes(as.factor(X..Event_type1....click..), fill = device_type_name)) +
      geom_bar(position= "fill") +
      stat_fill_labels() +
      labs( x = "Clique", y ="Pourcentage de click", fill = "Type d'appareil") +
      scale_y_continuous(labels = percent)
      
  })
  
  click_jour = clicks %>% 
    group_by(day, event_country) %>% 
    dplyr::summarise(total_click = n())
  
  
  win_jour = data %>% 
    group_by(day, event_country) %>% 
    dplyr::summarise(total_win = n())
  
  
  
  join_jour = inner_join(click_jour, win_jour , by = c("day" = "day", "event_country" = "event_country"))
  
  join_jour$pourcentage = join_jour$total_click/join_jour$total_win
  
  
  
  
  output$plot6 <- renderPlot({
    if(input$s2 == "All"){
      data_jour = join_jour
    }else{
      data_jour = join_jour %>% 
        filter(event_country == input$s2)
    }
    ggplot(data_jour, aes(x=day, y=pourcentage, fill = event_country)) +
      geom_bar(stat='identity')+
      labs(y ="Proportion des cliques", x = "Jours de la semaine") +
      scale_y_continuous(labels = percent)
  })
  
  

  
  
  
 
 ##total = device_make %>% 
  ##        group_by(event_country, event_device_make) %>% 
   ##       summarise(total_s = sum(total))
  
  #join = right_join(device_make , total, by = c("event_country", "event_device_make"))
  #join$pourcentage = round(join$total/ join$total_s * 100)
          
  
  
  
  


  
  
  
  
  
  
  
  
  
  
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
      creative = data
    }else{
      creative = data %>% 
        filter(event_country == input$s4)
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
    click
  })

  
  output$map_win <- renderLeaflet({
    win
  })
  
  
  
  
  
  
})
