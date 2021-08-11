'''
Nombre de archivo:
  +procesamientodatos.py
Descripción: 
  +Librería con funciones para el procesamiento de los datos
Métodos:
  |--+cargar_datos
  |--+generar_tablas
  |--+almacenar_tablas
'''

#librerías necesarias
import sys, os, datetime
from pyspark.sql import SparkSession, functions as F, window as W
from pyspark.sql.types import (DateType, IntegerType, FloatType, DoubleType, LongType, StringType, StructField, StructType, TimestampType)

#sesión de spark
spark = SparkSession.builder\
        .master("local")\
        .appName("App#1")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#función para carga de datos (lista de archivos .json)
def cargar_datos(files=[]):
  try:
    #lectura de archivos .json
    df1 = spark.read.json(files, multiLine=True)
    #se realizan las transformaciones necesarias para obtener cada uno de los elementos del esquema
    df1 = df1.withColumn('viajes', F.explode(F.col('viajes'))).select('identificador','viajes.*').orderBy('identificador')
    df1.collect()
    return [df1]
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función para generar las tablas con los resultados de los datos procesados
def generar_tablas(df=[]):
  try:
    #se crean dataframes temporales que sirven como tablas intermedias para el filtrado y agregación de los datos
    df1a = df[0].withColumnRenamed('codigo_postal_origen','codigo_postal').withColumn('tipo', F.lit('origen'))\
    .groupBy('codigo_postal', 'tipo').agg(F.count('codigo_postal').alias('cantidad_viajes'), F.sum(F.col('kilometros')*F.col('precio_kilometro')).alias('ingresos'))
    df1b = df[0].withColumnRenamed('codigo_postal_destino','codigo_postal').withColumn('tipo', F.lit('destino'))\
    .groupBy('codigo_postal', 'tipo').agg(F.count('codigo_postal').alias('cantidad_viajes'), F.sum(F.col('kilometros')*F.col('precio_kilometro')).alias('ingresos'))
    df1c = df[0].select('identificador', 'kilometros', 'precio_kilometro')\
    .groupBy('identificador').agg(F.sum('kilometros').alias('cantidad_kms'), F.sum(F.col('kilometros')*F.col('precio_kilometro')).alias('ingresos'))
    #tabla correspondiente a la cantidad de viajes por código postal
    df2 = df1a.union(df1b).select('codigo_postal', 'tipo', 'cantidad_viajes').orderBy(F.col('codigo_postal'), F.col('tipo').desc())
    #tabla correspondiente a los ingresos totales por código postal
    df3 = df1a.union(df1b).select('codigo_postal', 'tipo', F.round('ingresos',2).alias('ingresos')).orderBy(F.col('codigo_postal'), F.col('tipo').desc())
    #tabla correspondiente a la cantidad de kms e ingresos por identificador de conductor
    df4 = df1c.select('identificador', F.round('cantidad_kms',2).alias('cantidad_kms'), F.round('ingresos',2).alias('ingresos')).orderBy(F.col('identificador'))
    #tabla correspondiente a métricas particulares
    data = [('persona_con_mas_kilometros', df4.groupBy('identificador').agg(F.max('cantidad_kms')).orderBy(F.col('max(cantidad_kms)').desc()).collect()[0][0]),\
            ('persona_con_mas_ingresos', df4.groupBy('identificador').agg(F.max('ingresos')).orderBy(F.col('max(ingresos)').desc()).collect()[0][0]),\
            ('percentil_25', df4.select(F.percentile_approx('ingresos', .25)).collect()[0][0]),\
            ('percentil_50', df4.select(F.percentile_approx('ingresos', .50)).collect()[0][0]),\
            ('percentil_75', df4.select(F.percentile_approx('ingresos', .75)).collect()[0][0]),\
            ('codigo_postal_origen_con_mas_ingresos', df1a.groupBy('codigo_postal').agg(F.max('ingresos')).orderBy(F.col('max(ingresos)').desc()).collect()[0][0]),\
            ('codigo_postal_destino_con_mas_ingresos', df1b.groupBy('codigo_postal').agg(F.max('ingresos')).orderBy(F.col('max(ingresos)').desc()).collect()[0][0])]
    schema = StructType(\
                        [StructField('tipo_metrica',StringType()),
                         StructField('valor',StringType()),])
    df5 = spark.createDataFrame(data, schema)
    #se agregan los dataframes a una lista para la iteración
    proceso = [df2, df3, df5]
    #por medio de las funciones list-map-lambda se ejecutan las operaciones iterando sobre los dataframes creados
    list(map(lambda x: {x.printSchema(), x.show(50)}, proceso)) #se despliegan el esquema y los datos correspondientes a cada tabla
    return proceso
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función para almacenar los dataframes en formato .csv
def almacenar_tablas(df=[], files_name=[]):
  try:
    #escritura de los archivos
    csv_files=[]
    if (len(df)==len(files_name)):
      #se ejecutan las operaciones de escritura iterando sobre cada objeto
      list(map(lambda x, y: {x.write.csv(y, mode='overwrite')}, df, files_name))
      #se ejecuta una función de comprobación, leyendo cada archivo creado
      [csv_files.append(spark.read.csv(files_name[i])) for i in range(len(files_name))]
      if csv_files: print('Tablas almacenadas: '+ str(files_name))
    return csv_files
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)
#