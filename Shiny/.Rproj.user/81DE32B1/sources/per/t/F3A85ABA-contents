library(shinydashboard)



header <- dashboardHeader( title = "Ter Publicité")

sidebar <- dashboardSidebar(
  collapsed = TRUE,
  sidebarMenu(
  menuItem("Accueil", tabName = "acceuil", icon = icon("dashboard")),
  menuItem("Présentation", tabName = "presentation", icon = icon("far fa-address-card")),
  menuItem("Axe user", tabName = "user", icon = icon("far fa-chart-bar")),
  menuItem("Axe creative", tabName = "creative", icon = icon("th"))
  )
)



body <- dashboardBody(
  tags$head(
    tags$link(rel = "stylesheet", type = "text/css", href = "style.css")
  ),
  
  tabItems(
    tabItem(tabName = "acceuil",
            img(src='darren-chan.jpg')
 
    ),
    tabItem(tabName = "presentation",
            box(
              fluidRow(
                img(src ="logo_miashs.png", align = 'left', class = 'resize'),
                img(src = "logo_tabmo.png",align = 'right', class = 'resize')
              ),
  
              width = 12,
              h3("Membre du groupe:"),p("Joseph AKA BROU, Marwa ELATRACHE, Caroline Martin, Tharshika NAGARATNAM", "Omar SECK"),
              hr(),
              h3("Encadrants:"), p("Faustine BOUSQUET, El hassan DAOUDI, Samy ZAROUR"),
              hr(),
              h3("Jeu de données et source de données:"),p("Jeu de données sur une semaine fourni par TaBmo"),
              hr(),
              h3("Objectif"),p("Prédiction des cliques"),
              hr(),
              h3("Outils utilisés:"), p("Python, R, Trello, Google docs, Google collab")
              
            )
            
    ),
    tabItem(tabName = "user",
            
            fluidRow(
              #class = "color_box",
              box(
                title ="Analyse du profil des utilisateurs",
                status = 'warning',
                width = 12,
                collapsible = TRUE,
                selectInput("s2", label = h3("Pays"),
                            choices = list("All","ARE", "AUT", "BEL", "CAN",
                                           "CHE", "DEU", "ESP", "FRA",
                                           "GBR", "IRL", "ITA", "JPN",
                                           "MEX", "PHL", "THA", "USA")))
              ),
            fluidRow(
              box( width = 12,
                   collapsible = TRUE,
                   title =  "Nombre de win par pays",
                   plotOutput("plot5"))),
            
            fluidRow( box(title = "Proportion des cliques en fonction de l'heure" ,
                          collapsible = TRUE,
                          plotOutput("plot")),
                      box(
                        title ="Proportion des cliques en fonction du jour" ,
                        collapsible = TRUE,
                        plotOutput("plot6")),
                      box(
                        title = "Répartition du type d'OS en fonction des cliques( cliquée ou pas) ",
                        collapsible = TRUE,
                        plotOutput("plot2")),
                      box(
                        title = "Répartition du type d'appareil en fonction des cliques( cliquée ou pas) ",
                        collapsible = TRUE,
                        plotOutput("plot3"))),
                      
            fluidRow(
                   box(title = "Marque des appareils qui génèrent le plus de cliques",
                        collapsible = TRUE,
                        hr(),
                        DT::dataTableOutput("table2"))
            )
                      
            
             
    ),
    tabItem(tabName = "creative",
            fluidRow(
              box(
                title = "Analyse des publicités cliquées",
                width = 12,
                collapsible = TRUE,
                status = "warning",
                sliderInput("slider1", label = h3("Nombre de sites à afficher"),
                            min = 1, max = 50, value = 5),
                selectInput("s4", label = h3("Pays"),
                            choices = list("All","ARE", "AUT", "BEL", "CAN",
                                           "CHE", "DEU", "ESP", "FRA",
                                           "GBR", "IRL", "ITA", "JPN",
                                           "MEX", "PHL", "THA", "USA")))
              ),
            fluidRow(
              box(
                title = "les sites qui génèrent le plus de cliques",
                  collapsible = TRUE,
                  hr(),
                  DT::dataTableOutput("table")),
              box(
                title = "La répartition des publicités cliquées en fonction de leur taille", 
                collapsible = TRUE,
                plotOutput("plot4"))
              
          ),
          fluidRow(
                  
                  box(
                    title = "Proportion des publicités cliquées en fonction de leur prix",
                    width = 12,
                    plotOutput("plot7"))
          )
              
            )
           
    )
  )

dashboardPage(header,
              sidebar,
              body,
              skin = "black")






