library(shiny)
library(dplyr)
library(DT)
library(scales)
library(JLutils)
library(ggplot2)
library(plotly)
source("map.R")




shinyServer(function(input, output) {

  df = read.csv("data/one_month_data.csv")
  
  df$creative_size = factor(df$creative_size, levels = c("300x50","300x100","300x250","300x600", "320x50","320x100","320x480",
                                                         "480x320", "728x90" ,  "768x1024", "1024x768"))
  
  total_clicks = sum(df$click)
  

  
  test = df %>% 
    dplyr::group_by(Country_name,
                    country_code,
                    Timestamp_hour) %>% 
    dplyr::summarise(total_click = sum(click),
                     total_win = dplyr::n(),
                     taux = round(total_click/total_clicks,4)*100) %>% 
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
  
  
  

  output$plot2 <- renderPlotly({
    if(input$s2 == "All" ){
      dd = df
    }else{
      dd = filter(df, country_code %in% input$s2 )
    }
   ggplot(dd, aes(as.factor(click), fill = os)) +
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
  
  device_owner = df %>% 
    filter(click == 1) %>% 
    group_by(country_code, device_make) %>% 
    dplyr::summarise(total = n())%>%
    arrange(desc(total))
  
  
  output$table2 <-  DT::renderDataTable({
    if(input$s2 == "All" ){
      t =  device_owner
    }else{
      t= device_owner %>% 
        filter(country_code == input$s2) 
    }
    
    datatable(t, rownames = FALSE)
  })
  
  
  

  #Axe Creative
  
  sites_all = df %>% 
    filter(click == 1) %>% 
    group_by(country_code , app_site_name) %>%
    dplyr::summarise(total =n()) %>%
    arrange(desc(total))
  
  
  output$table <-  DT::renderDataTable({
    if(input$s4 == "All" ){
      table <- sites_all %>% 
        head(input$slider1)
    }else{
      table = sites_all %>% 
        filter(country_code == input$s4) %>% 
        head(input$slider1)
    }

    datatable(table, rownames = FALSE)
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
  
  max_price = round(max(df$win_price))
  df$win_interval = factor(df$win_interval, levels = c("0-0.2", "0.2-0.5", "0.5-0.8", "0.8-2","2-5", "5-10", "10-20", "20-30", paste("30-",max_price)) )
  

  
  
  output$plot7 <- renderPlot({
    
    if(input$s4 == "All"){
      prix = df
    }else{
      prix = df %>% 
        filter(country_code == input$s4)
    }
    
    ggplot(prix, aes(x=win_interval, fill = country_code)) +
      labs(x = "prix de l'enchère", y = "Pourcentage de cliques", fill = "Pays") +
      geom_bar(aes(y = (..count..)/sum(..count..))) +
      scale_y_continuous(labels = percent)
  
  })
  
  
  
  output$map_click  <- renderLeaflet({
    click_map
  })

})
