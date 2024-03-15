# Databricks notebook source
# MAGIC %md
# MAGIC # 1. IMPORTAR BIBLIOTECAS 

# COMMAND ----------

import requests, csv
import pandas as pd


# COMMAND ----------

url = 'https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial=%2701-01-2019%27&@dataFinalCotacao=%2712-31-2025%27&$top=9000&$format=text/csv&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao'
api_banco = pd.read_csv(url)
print(api_banco)

# COMMAND ----------

df=spark.createDataFrame(api_banco)
df.display()
