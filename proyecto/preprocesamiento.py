'''
Nombre de archivo:
  +preprocesamiento.py
Descripción: 
  +Librería de funciones para carga y pre-procesamiento de datos
Métodos:
  |--+crear_df
  |--+preprocesar_df
  |--+unir_df
  |--+feating
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
        #obtención de muestra aleatoria sin reemplazo (por defecto 100%)
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
            .withColumn('daywk', F.dayofweek(F.col('date')).cast('int'))\
            .withColumn('wkday', F.when(F.col('daywk')<5,1).otherwise(0))\
            .withColumn('month', F.month(F.col('date')).cast('int'))\
            .withColumn('sdephr', F.expr('substring(sdeptim, 1, length(sdeptim)-2)').cast('int'))\
            .withColumn('sarrhr', F.expr('substring(sarrtim, 1, length(sarrtim)-2)').cast('int'))\
            .withColumn('morning', F.when(F.col('sdephr')<12,1).otherwise(0))\
            .withColumn('label', F.when(F.col('arrdel')>0,1).otherwise(0))\
            .withColumn('carrier_cnt', F.count('carrier').over(W.Window.partitionBy('carrier')))\
            .withColumn('carrier_rnk', F.dense_rank().over(W.Window.orderBy(F.desc('carrier_cnt'))))\
            .withColumn('carrier', F.when(F.col('carrier_rnk')>9,'00').otherwise(F.col('carrier')))\
            .drop('daywk','carrier_cnt','carrier_rnk','sdephr','sdeptim','deptim','wofftim','wontim','txin','arrdel','arrtim','sarrtim','sarrhr','aelap','airtim')

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
            .drop('StartTime(UTC)','EndTime(UTC)')

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
    #imputación y selección de variables
    #se realiza la imputación de los campos nulos producto de la unión left
    #se realiza la selección final de las variables de interés
    df_jn = df_ar\
            .join(df_fl, on=[df_ar['iata']==df_fl['orig']], how='inner')\
            .join(df_wt, on=[df_ar['icao']==df_wt['icao'],
                  df_fl['date']==df_wt['date']], how='left')\
            .na.fill(value='Clear',subset=['wtyp'])\
            .na.fill(value='Calm',subset=['wsev'])\
            .drop('iata','icao','icao','date','date')\
            .na.drop(how='any')\
            .select('carrier','wkday','month','morning','wtyp','wsev',
                    'depdel','txout','selap','dist',
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

#ingeniería de características
'''
Determinaciones:
+ las clases se encuentran defindas y con un balance aceptable (35/65 aprox.)
+ el conjunto de datos presenta variables tanto categóricas como numéricas
+ las escalas de los valores difieren entre algunas columnas
+ se realiza un proceso de imputación para las variables numéricas
+ se realiza un proceso de imputación para las variables categóricas
+ se realiza un proceso de indexación y codificación para las variables categóricas
+ se realiza un proceso de vectorización para las variables de interés
+ se realiza un proceso de estandarización (se opta por el StandardScaler)
+ se realiza un proceso de extracción de columnas para almacenar en BD
'''
#función para aplicar técnicas de "feature engineering" 
def feating(df, print_=True):
  try:
    sf1, sf2, ef = '\n\033[1m\033[103m\033[30m', '\n\033[1m\033[106m\033[30m', '\033[0m'
    #análisis de variables numéricas y categóricas
    numvar, catvar, missfill = ['depdel','txout','selap','dist'], ['carrier','wkday','month','morning','wtyp','wsev'], {}
    imputer = Imputer(inputCols=numvar, outputCols=[var+'_imputed' for var in numvar])
    for c in numvar: df = df.withColumn(c, df[c].cast('double'))
    for var in catvar: missfill[var] = 'missing'
    df = imputer.fit(df).transform(df).fillna(missfill)
    #indexación y codificación de variables categóricas
    stgstridx = [StringIndexer(inputCol=c, outputCol=c+'_stridx') for c in catvar]
    stgonehot = [OneHotEncoder(inputCol=c+'_stridx', outputCol=c+'_onehot') for c in catvar]
    catppl = pipe(stages=stgstridx+stgonehot)
    dfenc = catppl.fit(df).transform(df)
    #vectorización
    cols = list(filter(lambda x: '_imputed' in x or '_onehot' in x, dfenc.columns))
    vecassem = VectorAssembler(inputCols=cols, outputCol='features', handleInvalid='skip')
    dfvec = vecassem.transform(dfenc)
    #estandarización
    stdscaler = StandardScaler(inputCol='features', outputCol='scaled', withStd=True, withMean=True)
    dfstd = stdscaler.fit(dfvec).transform(dfvec)
    #vector disperso a columnas
    c = dfstd.select('carrier_onehot','wkday_onehot','month_onehot','morning_onehot','wtyp_onehot','wsev_onehot').limit(1).collect()
    c = list(len(i.toArray()) for i in c[0])
    veccols = ['depdel','txout','selap','dist'] +\
              list('carrier_'+str(var+1) for var in range(c[0])) +\
              list('wkday_'+str(var+1) for var in range(c[1])) +\
              list('month_'+str(var+1) for var in range(c[2])) +\
              list('morning_'+str(var+1) for var in range(c[3])) +\
              list('wtyp_'+str(var+1) for var in range(c[4])) +\
              list('wsev_'+str(var+1) for var in range(c[5])) +\
              ['label']
    dfcols = spark.createDataFrame(pd.DataFrame(np.array(list(np.append(s.toArray(), l) for s,l in dfstd.select('scaled','label').collect())), columns=veccols))

    if print_:
      plot_corr(df, inputcols=['wkday','month','morning','depdel','txout','selap','dist'])
      print(sf1, 'Conjunto con variables procesadas', ef)
      dfstd.select('scaled','label').show(10)
      dfstd.printSchema()

    # #minmax (opción)
    # minmaxer = MinMaxScaler(inputCol='features', outputCol='minmaxed', min=0., max=1.)
    # dfminmax = minmaxer.fit(dfvec).transform(dfvec)
    # dfminmax = dfminmax.select(['minmaxed', 'label'])
    # dfminmax.show(10, truncate=False)

    # #normalización (opción)
    # normalizer = Normalizer(inputCol='features', outputCol='normed', p=2.0)
    # dfnormed = normalizer.transform(dfvec)
    # dfnormed = dfnormed.select(['normed', 'label'])
    # dfnormed.show(10, truncate=False)

    return (dfstd, dfcols)
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)