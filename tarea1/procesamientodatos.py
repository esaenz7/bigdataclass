'''
Nombre de archivo:
  +procesamientodatos.py
Descripción: 
  +Librería con funciones para el procesamiento de los datos
Métodos:
  +cargar_datos
  +unir_datos
  +agregar_datos
  +presentar_datos
  +almacenar_datos
'''

#librerías necesarias
import sys, os, datetime
from pyspark.sql import SparkSession, functions as F, window as W
from pyspark.sql.types import (DateType, IntegerType, FloatType, DoubleType, StringType, StructField, StructType, TimestampType)

#sesión de spark
spark = SparkSession.builder\
        .master("local")\
        .appName("App#1")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#función para carga de datos (lista con archivos csv)
def cargar_datos(files=[], show=20, print_=True):
  try:
    #lectura de archivos a partir de la definición de esquemas
    df1 = spark.read.csv(files[1], schema=StructType(\
                                  [StructField('cedula', IntegerType()),
                                  StructField('nombre', StringType()),
                                  StructField('provincia', StringType()),]))
    df2 = spark.read.csv(files[2], schema=StructType(\
                                  [StructField('codigo_ruta', IntegerType()),
                                  StructField('nombre_ruta', StringType()),
                                  StructField('kms', FloatType()),]))
    df3 = spark.read.csv(files[3], schema=StructType(\
                                  [StructField('codigo_ruta', IntegerType()),
                                  StructField('cedula', IntegerType()),
                                  StructField('fecha', DateType()),]))
    #impresión de resultados
    if print_:
      print('DataFrame1')
      df1.show(show)
      df1.printSchema()
      print('DataFrame2')
      df2.show(show)
      df2.printSchema()
      print('DataFrame3')
      df3.show(show)
      df3.printSchema()
    return [df1, df2, df3]
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función para unión de datos de los dataframes (data=lista de dataframes) y selección de las columnas requeridas (select=lista de columnas)
def unir_datos(data=[], select=[], show=20, print_=True):
  try:
    #unión de los dataframes a partir de las columnas relacionadas
    dfResultados1 = data[2].join(data[1], data[1].codigo_ruta == data[2].codigo_ruta)\
    .join(data[0], data[0].cedula == data[2].cedula)
    #selección de las columnas requeridas
    dfResultados2 = dfResultados1.select(select).dropna()
    #impresión de resultados
    if print_:
      dfResultados1.show(show)
      dfResultados2.show(show)
    return [dfResultados1, dfResultados2]
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función para agrupar y agregar los datos (data=dataframe) a partir de las columnas especificadas (group=columnas a agrupar, agg=columna de agregación)
def agregar_datos(data=[], group=[], agg='', show=20, print_=True):
  try:
    #agrupación y agregación de los datos (totales y promedios)
    dfResultados3 = data[1].groupBy(group).agg(F.sum(agg),F.mean(agg))\
    .withColumn('sum('+agg+')', F.round('sum('+agg+')',2))\
    .withColumn('avg('+agg+')', F.round('avg('+agg+')',2))
    #impresión de resultados
    if print_:
      dfResultados3.show(show)
    return [dfResultados3]
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función para presentar los datos (data=dataframe) particionados (part=columna) y ordenados (order=columna) con un límite (top=cantidad de filas)
def presentar_datos(data=[], top=5, part='', order='', show=20, print_=True):
  try:
    #definición de operación de partición y ordenamiento
    window = W.Window.partitionBy(part).orderBy(F.col(part).desc(), F.col(order).desc())
    #partición y ordenamiento de los datos
    dfResultados4 = data[0].withColumn('row',F.row_number().over(window))\
    .filter(F.col('row')<=top).drop('row')
    #impresión de resultados
    if print_:
      print('\nDataFrame: Top 5 total de kms y promedio de kms diario, por provincia.')
      dfResultados4.show(show)
      print('\nEsquema del dataframe.')
      dfResultados4.printSchema()
      print('\nExplicación de ejecución de spark.')
      dfResultados4.explain()
    return [dfResultados4]
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función para guardar los datos (data=dataframe) con nombre (nombre=nombre del archivo)
def almacenar_datos(data=[], nombre='default.csv', show=20, print_=True):
  try:
    #escritura de archivo
    data[0].write.csv(nombre, mode='overwrite')
    #lectura de archivo guardado
    dfResultados5 = spark.read.csv(nombre, schema=data[0].schema)
    #impresión de resultados
    if print_:
      print('\nDataFrame: obtenido del archivo '+nombre+'.')
      dfResultados5.show(show)
      print('\nDataFrame: descripción de los datos por provincia.')
      dfResultados5.groupby(dfResultados5[0]).agg(F.count(dfResultados5[2]).alias('count'),F.min(dfResultados5[2]).alias('min'),F.max(dfResultados5[2]).alias('max'),F.round(F.mean(dfResultados5[2]),2).alias('avg')).show()
    return [dfResultados5]
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)