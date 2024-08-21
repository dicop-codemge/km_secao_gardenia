##CARREGANDO QDMP RESUMO NOS BD GEPPI
# Verificando e instalando as libraries necessárias
if (!require("RPostgres")) install.packages("RPostgres")
if (!require("openxlsx")) install.packages("openxlsx")
if (!require("stringr")) install.packages("stringr")
if (!require("dplyr")) install.packages("dplyr")
if (!require("gdata")) install.packages("gdata")
if (!require("readxl")) install.packages("readxl")
if (!require("data.table")) install.packages("data.table")
if (!require("osrm")) install.packages("osrm")
if (!require("sf")) install.packages("sf")

#LIBRARIES
library(RPostgres)
library(openxlsx)
library(stringr)
library(dplyr)
library(gdata)
library(readxl)
library(data.table)
library(osrm)
library(sf)

#CARREGANDO DATAFRAMES
#CRIANDO CONEXÃO COM BD GEPPI
dsn_hostname <- Sys.getenv("DB_HOST")
dsn_port <- Sys.getenv("DB_PORT")
dsn_database <- Sys.getenv("DB_NAME")
dsn_uid <- Sys.getenv("DB_USER")
dsn_pwd <- Sys.getenv("DB_PASSWORD")


con <- dbConnect(RPostgres::Postgres(),
                 dbname = dsn_database,
                 host = dsn_hostname, port = 5432,
                 user = dsn_uid, password = dsn_pwd)
rm(dsn_database,dsn_hostname,dsn_port,dsn_uid,dsn_pwd)

stops<-st_read(con,query="SELECT * FROM transporte_intermunicipal.gtfs_stops")

query <- "
  SELECT * 
  FROM transporte_intermunicipal.extensao_od 
  WHERE origin_id IN (
    SELECT ponto_ini 
    FROM transporte_intermunicipal.tb_intermun 
    WHERE servico IN (
      SELECT cod_linha 
      FROM transporte_intermunicipal.linha_intermun 
      WHERE cod_delegatario = '9096'
    )
  ) 
  AND destination_id IN (SELECT ponto_fim 
    FROM transporte_intermunicipal.tb_intermun 
    WHERE servico IN (
      SELECT cod_linha 
      FROM transporte_intermunicipal.linha_intermun 
      WHERE cod_delegatario = '9096'
    )
  )
"

extensao_od <- dbGetQuery(con, query)
summary(extensao_od)

rota_completa <- list()
j<-1
for (j in 1:nrow(extensao_od)) {
  tryCatch({
    origem_sf <- filter(stops, stops$stop_id==extensao_od$origin_id[j])
    destino_sf <- filter(stops, stops$stop_id==extensao_od$destination_id[j])
    rota_trecho <- osrmRoute(src = origem_sf, dst = destino_sf, overview = 'full')
    rota_trecho$origin <- origem_sf$stop_id
    rota_trecho$destination <- destino_sf$stop_id
    rota_completa <- append(rota_completa, list(rota_trecho))
  }, error = function(e){
    cat("Erro na linha", j, "- pulando para a próxima iteração\n")
  })
}

# Combinar todas as rotas de uma vez
rota_completa <- do.call(rbind, rota_completa)
rota_completa<- as.data.frame(rota_completa)
write.csv2(rota_completa,"rota_completa.csv",row.names = F)



