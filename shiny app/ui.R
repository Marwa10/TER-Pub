library(shinydashboard)
library(leaflet)
library(plotly)



header <- dashboardHeader( title = "TER Publicité")

sidebar <- dashboardSidebar(
  collapsed = TRUE,
  sidebarMenu(
  menuItem("Accueil", tabName = "acceuil", icon = icon("fas fa-home")),
  menuItem("Présentation", tabName = "presentation", icon = icon("far fa-address-card")),
  menuItem("Axe utilisateur", tabName = "user", icon = icon("far fa-chart-bar")),
  menuItem("Axe créative", tabName = "creative", icon = icon("far fa-chart-bar")),
  menuItem("Cartographie", tabName = "carto", icon =  icon("fas fa-map-marked-alt")),
  menuItem("Modèles", tabName = "modeles", icon = icon("fas fa-laptop-code"))
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
            fluidRow(
            width = 12,
            height = 1000,
            box(
              width = 12,
              fluidRow(
                img(src ="logo_miashs.png", align = 'left', class = 'resize'),
                img(src = "logo_tabmo.png",align = 'right', class = 'resize')
              ),
              h3("Contexte"), 
              p ("TabMo est une société devenue leader sur le marché de la publicité programmatique Mobile en développant son DSP Mobile « Hawk »,
                   offrant aux groupes médias la possibilité d’acheter leur inventaire publicitaire Mobile, TV et DOOH en temps réel.
              Le développement de cette plateforme est assuré par l’équipe R&D au sein de notre bureau de Montpellier."),
             p("Au sein de l’équipe Data (Science et Engineering), nous avons pour objectif de stocker et apporter de la valeur ajoutée à la plateforme en
                                 utilisant à bon escient, et dans le respect des législations en vigueur, les données qui transitent sur Hawk (données relatives aux publicités diffusées,
                                 au support sur lequel la publicité est diffusée, le nombre de clics effectifs…). Ces données volumineuses et hétérogènes nous permettent
                                 de développer des méthodes statistiques qui répondent à des problématiques diverses ; par exemple la prédiction du taux de clics en temps réel."),
             hr(),
             h3("Membre du groupe:"),p("Joseph AKA BROU, Marwa ELATRACHE, Caroline MARTIN, Tharshika NAGARATNAM  et Omar SECK"),
              hr(),
              h3("Encadrants:"), p("Faustine BOUSQUET, El hassan DAOUDI, Samy ZAROUR"),
              hr(),
              h3("Jeu de données et source de données:"),p("Jeu de données sur une semaine fourni par TaBmo"),
              hr(),
              h3("Objectif"),p("Prédiction des clics"),
              hr(),
              h3("Outils utilisés:"), p("Python, R, Trello, Github, Google docs, Google colab, Scala,  Slack")
              
            ))
            
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
                            choices = list("All","AGO","ARE","AUS","AUT","BEL","BGD","BHR",
                                           "CAN","CHE","CHN","CIV","COL","DEU","DNK",
                                           "DZA","ECU","EGY","ESP","FIN","FRA","GBR","GGY",
                                           "GLP","GTM","HKG","HND","IDN","IMN","IND","IRL",
                                           "IRN","IRQ","ISR","ITA","JEY","JPN","KAZ","KWT",
                                           "LBN","LUX","MAR","MCO","MEX","MTQ","NLD","NOR",
                                           "NPL","OMN","PAK","PER","POL","PRT","QAT","REU",
                                           "RUS","SAU","SOM","SWE","SYR","TGO","THA","TUR",
                                           "TWN","USA","VNM","YEM")))
              ),
            fluidRow(
              box( width = 12,
                   collapsible = TRUE,
                   title =  "Nombre de win par pays",
                   plotlyOutput("plot5"))),
            
            fluidRow( box(title = "Proportion des clics en fonction de l'heure" ,
                          collapsible = TRUE,
                          plotlyOutput("plot")),
                      box(
                        title ="Proportion des clics en fonction du jour" ,
                        collapsible = TRUE,
                        plotlyOutput("plot6")),
                      box(
                        title = "Répartition du type d'OS en fonction des clics( cliquée ou pas) ",
                        collapsible = TRUE,
                        plotOutput("plot2")),
                      box(
                        title = "Répartition du type d'appareil en fonction des clics( cliquée ou pas) ",
                        collapsible = TRUE,
                        plotlyOutput("plot3"))),
                      
            fluidRow(
                   box(title = "Marque des appareils qui génèrent le plus de clics",
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
                            choices = list("All","AGO","ARE","AUS","AUT","BEL","BGD","BHR",
                                           "CAN","CHE","CHN","CIV","COL","DEU","DNK",
                                           "DZA","ECU","EGY","ESP","FIN","FRA","GBR","GGY",
                                           "GLP","GTM","HKG","HND","IDN","IMN","IND","IRL",
                                           "IRN","IRQ","ISR","ITA","JEY","JPN","KAZ","KWT",
                                           "LBN","LUX","MAR","MCO","MEX","MTQ","NLD","NOR",
                                           "NPL","OMN","PAK","PER","POL","PRT","QAT","REU",
                                           "RUS","SAU","SOM","SWE","SYR","TGO","THA","TUR",
                                           "TWN","USA","VNM","YEM")))
              ),
            fluidRow(
              box(
                title = "les sites qui génèrent le plus de clics",
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
              
            ),
    tabItem(tabName = "carto",
              box(
                title = "Distribution géographique des clics et win ",
                width = 12,
                leafletOutput("map_click", height = "600px")
              )
    ),
    tabItem(tabName = "modleles")
           
    )
  )

dashboardPage(header,
              sidebar,
              body,
              skin = "black")






