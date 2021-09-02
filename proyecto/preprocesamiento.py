'''
Nombre de archivo:
  +preprocesamiento.py
Descripción: 
  +Librería de funciones para carga y pre-procesamiento de datos
Métodos:
  |--+crear_df
  |--+preprocesar_df
  |--+unir_df
  |--+escribir_df
  |--+leer_df
'''

#librerías
from recursos import *

#función para crear conjuntos de datos (dataframes) a partir de archivos
def crear_df(paths=[], formats=[], headers=[], samples_fr=[1.], rand_st=None, print_=True):
  try:
    sf1, sf2, ef = '\n\033[1m\033[103m\033[30m', '\n\033[1m\033[106m\033[30m', '\033[0m'
    if (len(paths)==len(formats)) & (len(paths)==len(headers)) & (len(paths)==len(samples_fr)):
      df_list = []
      for i, (path, format, header, sample_fr) in enumerate(zip(paths, formats, headers, samples_fr)):
        # i = i+1 # globals()['dfi'+str(i)]
        #lectura de archivos y carga del dataframe
        dfi = spark \
          .read \
          .format(format) \
          .option('path', path) \
          .option('header', header) \
          .option('inferSchema', True) \
          .load()
        #obtención de muestra aleatoria
        dfs = dfi.sample(sample_fr, rand_st)

        #despliegue de resultados
        if print_:
          #detalles del dataframe
          print(sf1,'Dataframe', i+1, ef, '(', path, ')')
          # print('Archivo: [', dfi.count(), 'filas,', len(dfi.columns), 'columnas ]')
          # print('Muestra: [', dfs.count(), 'filas,', len(dfs.columns), 'columnas ]')
          dfs.show(10, truncate=False)
          dfs.printSchema()
          print(sf2, 'Momentos estadísticos', ef)
          dfs.describe().show()
        df_list.append(dfs)
      return df_list
    else:
      print('Los parámetros tipo listas deben tener la misma cantidad de items.')
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función para preprocesar los conjuntos de datos
def preprocesar_df(df_list, print_=True):
  try:
    sf1, sf2, ef = '\n\033[1m\033[103m\033[30m', '\n\033[1m\033[106m\033[30m', '\033[0m'
    #con base en el análisis preliminar de los datos se realiza un procesamiento inicial de los subconjuntos
    #esto implica tareas de selección, transformación y filtrado sobre las variables de interés

    #subconjunto de interés "flights"
    df_fl = df_list[0]\
            .filter((F.col('CANCELLED') == 0) & (F.col('DIVERTED') == 0))\
            .select(
                    F.to_date('FL_DATE').alias('date'),
                    F.col('ORIGIN').alias('orig'),
                    F.col('DEST').alias('dest'),
                    F.col('OP_CARRIER').alias('carrier'),
                    F.col('CRS_DEP_TIME').cast('int').alias('sdeptim'),
                    F.col('DEP_TIME').cast('int').alias('deptim'),
                    F.col('DEP_DELAY').cast('int').alias('depdel'),
                    F.col('TAXI_OUT').cast('int').alias('txout'),
                    F.col('WHEELS_OFF').cast('int').alias('wofftim'),
                    F.col('WHEELS_ON').cast('int').alias('wontim'),
                    F.col('TAXI_IN').cast('int').alias('txin'),
                    F.col('ARR_DELAY').cast('int').alias('arrdel'),
                    F.col('ARR_TIME').cast('int').alias('arrtim'),
                    F.col('CRS_ARR_TIME').cast('int').alias('sarrtim'),
                    F.col('CRS_ELAPSED_TIME').cast('int').alias('selap'),
                    F.col('ACTUAL_ELAPSED_TIME').cast('int').alias('aelap'),
                    F.col('AIR_TIME').cast('int').alias('airtim'),
                    F.col('DISTANCE').cast('int').alias('dist'))\
            .withColumn('dyofwk', F.dayofweek(F.col('date')).cast('int'))\
            .withColumn('wkofyr', F.weekofyear(F.col('date')).cast('int'))\
            .withColumn('sdephr', F.expr('substring(sdeptim, 1, length(sdeptim)-2)').cast('int'))\
            .withColumn('sarrhr', F.expr('substring(sarrtim, 1, length(sarrtim)-2)').cast('int'))\
            .withColumn('label', F.when(F.col('arrdel')>0,1).otherwise(0))\
            .drop('deptim', 'wofftim', 'wontim', 'txin', 'arrdel', 'arrtim', 'aelap', 'airtim')

    #subconjunto de interés "airports"
    df_ar = df_list[1]\
            .filter(F.col('_c3')=='United States')\
            .select(
                    F.col('_c4').alias('iata'),
                    F.col('_c5').alias('icao'))

    #subconjunto de interés "weather"
    df_wt = df_list[2]\
            .filter(F.col('StartTime(UTC)').between('2018-01-01 00:00:00','2019-01-01 00:00:00'))\
            .select(
                    F.to_date('StartTime(UTC)').alias('date'),
                    F.col('Type').alias('wtyp'),
                    F.col('Severity').alias('wsev'),
                    F.col('AirportCode').alias('icao'),
                    F.col('StartTime(UTC)'),
                    F.col('EndTime(UTC)'))\
            .withColumn('evhr', F.hour(F.col('StartTime(UTC)')))\
            .withColumn('evtim', ((F.unix_timestamp(F.col('EndTime(UTC)')) - 
                                   F.unix_timestamp(F.col('StartTime(UTC)')))/60).cast('int'))\
            .drop('StartTime(UTC)', 'EndTime(UTC)')

    #despliegue de resultados
    if print_:
      print(sf1, 'Preparación de los conjuntos de datos', ef)
      print(sf2, 'Subconjunto de interés "flights"', ef)
      # print('[', df_fl.count(), 'filas,', len(df_fl.columns), 'columnas ]')
      df_fl.show(10, truncate=False)
      df_fl.printSchema()
      print(sf2, 'Subconjunto de interés "airports"', ef)
      # print('[', df_ar.count(), 'filas,', len(df_ar.columns), 'columnas ]')
      df_ar.show(10, truncate=False)
      df_ar.printSchema()
      print(sf2, 'Subconjunto de interés "weather"', ef)
      # print('[', df_wt.count(), 'filas,', len(df_wt.columns), 'columnas ]')
      df_wt.show(10, truncate=False)
      df_wt.printSchema()
    return [df_fl, df_ar, df_wt]
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función para unir los conjuntos de datos
def unir_df(df_listready, print_=True):
  try:
    sf1, sf2, ef = '\n\033[1m\033[103m\033[30m', '\n\033[1m\033[106m\033[30m', '\033[0m'
    #se realiza la unión de los subconjuntos
    #con base en las estadísticas descriptivas se realizan los procesos de imputación de datos
    df_fl, df_ar, df_wt = df_listready
    #unión de subconjuntos "flights-airports-weather"
    #se realiza una unión tipo "left" para mantener todos los registros de vuelos aún cuando no hayan registros de eventos meteorológicos
    df_jn = df_ar\
            .join(df_fl, on=[df_ar['iata']==df_fl['orig']], how='inner')\
            .join(df_wt, on=[df_ar['icao']==df_wt['icao'],
                  df_fl['date']==df_wt['date']], how='left')\
            .drop('iata','icao','icao','date','date')
    #imputación y selección de variables
    #se realiza la imputación de los campos nulos producto de la unión left
    #se realiza la selección final de las variables de interés
    df_jn = df_jn\
            .na.fill(value='Clear',subset=['wtyp'])\
            .na.fill(value='Calm',subset=['wsev'])\
            .na.fill(value=0,subset=['evhr','evtim'])\
            .na.fill(value=0,subset=['sdephr','sarrhr'])\
            .na.drop(how='any')\
            .select('carrier','sdephr','sarrhr','dyofwk','wkofyr','wtyp','wsev',
                    'depdel','txout','selap','dist','evtim',
                    'label')

    #despliegue de resultados
    if print_:
      print(sf1, 'Conjunto de datos preparado', ef)
      # print('[', df_jn.count(), 'filas,', len(df_jn.columns), 'columnas ]')
      df_jn.show(10, truncate=False)
      df_jn.printSchema()
      print(sf2, 'Momentos estadísticos', ef)
      df_jn.describe().show()
      print(sf2, 'Coeficiente de valores nulos', ef)
      df_jn.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in df_jn.columns]).show()
      print(sf2, 'Balance de clases objetivo', ef)
      dftarget = df_jn.groupBy('label').count()
      dftarget = dftarget.withColumn('%', F.round(dftarget['count']*100/df_jn.count(),2)).show()
    return df_jn
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)