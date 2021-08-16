'''
Nombre de archivo:
  +programaextra.py
Descripción: 
  +Archivo principal (main) para la ejecución del programa extra
'''

#librerías necesarias
import sys, os
from procesamientodatos import *
from pyspark.sql import SparkSession, functions as F
from datetime import datetime

args = sys.argv
print(args)
host = args[1]
port = args[2]
user = args[3]
password = args[4]
dbtable = args[5]
files = [x for x in args if '.json' in x]

#llamado a las funciones de procesamientodatos
dfinicial = cargar_datos(files)
proceso = generar_tablas(dfinicial)
df_metricas = proceso[3] #dataframe metricas

#sesión spark
spark = SparkSession.builder.appName("PSQL Transactions").getOrCreate()

#conversión de fecha
string_to_date = F.udf(lambda text_date: datetime.strptime(text_date, '%Y/%m/%d'), DateType())
df_metricas = df_metricas.withColumn("fecha", string_to_date(df_metricas.fecha))

#escribir datos en postgres
df_metricas\
    .write.mode("Append") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://"+host+":"+port+"/postgres") \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", dbtable) \
    .save()

#leer datos de postgres
dataframe = spark \
    .read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://"+host+":"+port+"/postgres") \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", dbtable) \
    .load()

print('Postgres dbtable metricas:\n')
dataframe.show(50, truncate=False)
dataframe.printSchema()
