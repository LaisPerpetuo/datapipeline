from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time



# setup da aplicação Spark
spark = SparkSession \
    .builder \
    .appName("etl-spark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")


# lendo os dados do Data Lake
df = spark.read\
    .option("header", "True")\
    .option("inferSchema","True")\
    .csv("s3a://landing/*.csv")
    


# imprime os dados lidos da landing
print ("\nImprime os dados lidos da landing:")
print (df.show())

# imprime o schema do dataframe
print ("\nImprime o schema do dataframe lido da landing:")
print (df.printSchema())

# converte para formato parquet
print ("\nEscrevendo os dados lidos da landing para parquet na processing zone...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("s3a://processing/df-parquet-file.parquet")

# lendo arquivos parquet
df_parquet = spark.read.format("parquet")\
 .load("s3a://processing/df-parquet-file.parquet")

# imprime os dados lidos em parquet
print ("\nImprime os dados lidos em parquet da processing zone")
print (df_parquet.show())

# cria uma view para trabalhar com sql
df_parquet.createOrReplaceTempView("view_df_parquet")

# processa os dados conforme regra de negócio
df_result = spark.sql("SELECT BNF_CODE as Bnf_code \
                       ,SUM(ACT_COST) as Soma_Act_cost \
                       ,SUM(QUANTITY) as Soma_Quantity \
                       ,SUM(ITEMS) as Soma_items \
                       ,AVG(ACT_COST) as Media_Act_cost \
                      FROM view_df_parquet \
                      GROUP BY bnf_code")

# imprime o resultado do dataframe criado
print ("\n ========= Imprime o resultado do dataframe processado =========\n")
print (df_result.show())

# converte para formato parquet
print ("\nEscrevendo os dados processados na Curated Zone...")

# converte os dados processados para parquet e escreve na curated zone
df_result.write.format("parquet")\
         .mode("overwrite")\
         .save("s3a://curated/df-result-file.parquet")

print("Dados armazenados com sucesso na Curated Zone")

# para a aplicação
spark.stop()