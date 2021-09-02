'''
Nombre de archivo:
  +recursos.py
Descripción: 
  +Librería con recursos varios para la aplicación
'''

#librerías
from IPython.display import Javascript
import sys, os, glob, datetime as dt, numpy as np, collections as coll
import pandas as pd, seaborn as sns, matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc
from pyspark.sql import SparkSession, functions as F, window as W, DataFrame as DF
from pyspark.sql.types import (DateType, IntegerType, FloatType, DoubleType, LongType, StringType, StructField, StructType, TimestampType)
from pyspark.ml import functions as mlF, Pipeline as pipe
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import Imputer, StandardScaler, MinMaxScaler, Normalizer, PCA, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, DecisionTreeClassificationModel, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder
from functools import reduce
import findspark
# findspark.init('/usr/lib/python3.7/site-packages/pyspark')
# !pip install -q handyspark
# from handyspark import *

#variables postgres
# args = sys.argv
# print(args)
#estos parámetros corresponden a la instancia de postgres dentro del ambiente de docker que se adjunta al trabajo
host = '10.7.84.102'
port = '5432'
user = 'postgres'
password = 'testPassword'

#sesión de spark
spark = SparkSession.builder\
  .master("local")\
  .appName("Main")\
  .config('spark.ui.port', '4050')\
  .config("spark.driver.extraClassPath", "postgresql-42.2.14.jar") \
  .config("spark.executor.extraClassPath", "postgresql-42.2.14.jar") \
  .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#funciones
#función para almacenar en base de datos
def escribir_df(df, host, port, user, password, table):
  try:
    #almacenamiento en base de datos
    df \
      .write \
      .format("jdbc") \
      .mode('overwrite') \
      .option("url", "jdbc:postgresql://"+host+":"+port+"/postgres") \
      .option("user", user) \
      .option("password", password) \
      .option("dbtable", table) \
      .save()
    return True
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)
'''
La ejecución de spark es una ejecución vaga ("lazy"), si se intenta almacenar un dataframe en una tabla la cual es a su vez su propia fuente de datos, 
dicha tabla será sobreescrita con valores nulos quedando vacía, por lo tanto en dichos casos se recomienda utilizar una tabla temporal.
'''
#función para cargar de base de datos
def leer_df(host, port, user, password, table):
  try:
    #lectura desde base de datos hacia dataframe temporal
    df = spark \
      .read \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://"+host+":"+port+"/postgres") \
      .option("user", user) \
      .option("password", password) \
      .option("dbtable", table) \
      .load()
    df.count()
    return df
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función columnas-vector
def cols2vec(dfin, inputcols=[], outputcol='features', label='label', lab_alias='label', print=False):
  try:
    assy = VectorAssembler(inputCols=inputcols, outputCol=outputcol)
    dfout = assy.transform(dfin)
    if lab_alias:
      dfout = dfout.select([outputcol, F.col(label).alias(lab_alias)])
    else:
      dfout = dfout.select([outputcol])
    if print: dfout.show(10, truncate=False)
    return dfout
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función vector-columnas
def vec2cols(dfin, inputcol='features', outputcols=[], label='label', lab_alias='label', print=False, prediction=None):
  try:
    if lab_alias:
      if prediction:
        dfout = dfin.select(inputcol, label, prediction).withColumn('temp', mlF.vector_to_array(inputcol)) \
        .select([F.col('temp')[i].alias(outputcols[i]) for i in range(len(outputcols))] + [F.col(label).alias(lab_alias)] + [F.col(prediction)])
      else:
        dfout = dfin.select(inputcol, label).withColumn('temp', mlF.vector_to_array(inputcol)) \
        .select([F.col('temp')[i].alias(outputcols[i]) for i in range(len(outputcols))] + [F.col(label).alias(lab_alias)])
    else:
      dfout = dfin.select(inputcol, label).withColumn('temp', mlF.vector_to_array(inputcol)) \
      .select([F.col('temp')[i].alias(outputcols[i]) for i in range(len(outputcols))])
    if print: dfout.show(10, truncate=False)
    return dfout
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función para aplicar técnicas de "feature engineering" 
def feating(df):
  try:
    #análisis de variables numéricas
    numvar = ['depdel','txout','selap','dist','evtim']
    for c in numvar:
      df = df.withColumn(c, df[c].cast('double'))
    numimp = [var + '_imputed' for var in numvar]
    imputer = Imputer(inputCols=numvar, outputCols=numimp)
    df = imputer.fit(df).transform(df)
    #análisis de variables categóricas
    catvar, missfill = ['carrier','sdephr','sarrhr','dyofwk','wkofyr','wtyp','wsev'], {}
    for var in catvar:
      missfill[var] = 'missing'
    df = df.fillna(missfill)

    #indexación y codificación de variables categóricas
    stgstridx = [StringIndexer(inputCol=c, outputCol=c+'_stridx') for c in catvar]
    stgonehot = [OneHotEncoder(inputCol=c+'_stridx', outputCol=c+'_onehot') for c in catvar]
    ppl = pipe(stages=stgstridx+stgonehot)
    df = ppl.fit(df).transform(df)
    print('Conjunto con variables procesadas')
    df.select(['depdel_imputed','txout_imputed','selap_imputed','dist_imputed','evtim_imputed',
                'carrier_onehot','sdephr_onehot','sarrhr_onehot','dyofwk_onehot','wkofyr_onehot','wtyp_onehot','wsev_onehot','label'])\
                .show(10, truncate=False)

    #vectorización
    dfvec = cols2vec(df, inputcols=['depdel_imputed','txout_imputed','selap_imputed','dist_imputed','evtim_imputed',
                                    'carrier_onehot','sdephr_onehot','sarrhr_onehot','dyofwk_onehot','wkofyr_onehot',
                                    'wtyp_onehot','wsev_onehot'],
                    outputcol='features', label='label', lab_alias='label', print=False)

    #estandarización
    stdscaler = StandardScaler(inputCol='features', outputCol='scaled', withStd=True, withMean=True).fit(dfvec)
    dfscaled = stdscaler.transform(dfvec)
    dfscaled = dfscaled.select(['scaled', 'label'])
    print('Conjunto con variables vectorizadas y estandarizadas')
    dfscaled.show(10, truncate=False)

    # #minmax
    # minmaxer = MinMaxScaler(inputCol='features', outputCol='minmaxed', min=0., max=1.).fit(dfvec)
    # dfminmax = minmaxer.transform(dfvec)
    # dfminmax = dfminmax.select(['minmaxed', 'label'])
    # dfminmax.show(10, truncate=False)

    # #normalización
    # normalizer = Normalizer(inputCol='features', outputCol='normed', p=2.0)
    # dfnormed = normalizer.transform(dfvec)
    # dfnormed = dfnormed.select(['normed', 'label'])
    # dfnormed.show(10, truncate=False)

    # #reducción y selección de características mediante PCA
    # k=50
    # pca = PCA(k=k, inputCol='scaled', outputCol='pca').fit(dfscaled)
    # dfpca = pca.transform(dfscaled)
    # dfpca = dfpca.select(['pca','label'])
    # dfpca.show(10, truncate=False)
    # print('PCA, varianza explicada: ', pca.explainedVariance.toArray().sum(), ' = ', pca.explainedVariance.toArray())
    return dfscaled
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función de graficación de correlaciones
def plot_corr(df=None, inputcols=[]):
  try:
    dfcorr = cols2vec(df, inputcols=inputcols, outputcol='features')
    dfcorr = StandardScaler(inputCol='features', outputCol='scaled', withStd=True, withMean=True).fit(dfcorr).transform(dfcorr).select(['scaled', 'label'])
    print('Mapa de calor')
    pearson_matrix = Correlation.corr(dfcorr, column='scaled', method='pearson').collect()[0][0]
    sns.heatmap(pearson_matrix.toArray(), annot=True, fmt=".2f", cmap=sns.diverging_palette(255,10,as_cmap=True))
    plt.show()
    print('Gráfico de parcela')
    sns.pairplot(df.select(inputcols).toPandas(), height=2, aspect=16/9, corner=True)
    plt.show()
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función de graficación ROC
def plot_roc(df=None, ver=1, metric=None):
  try:
    getval = lambda x: [i[0] for i in x]
    getroc = lambda x,y: roc_curve(np.array(getval(x)), np.array(getval(y))[:,[1]].reshape(-1), pos_label=1)
    fpr, tpr, thresholds = getroc(df.select(['label']).collect(), df.select(['probability']).collect())
    roc_auc = auc(fpr, tpr)
    if ver==1:
      plt.figure(figsize=(5,5))
      plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
      plt.plot([0, 1], [0, 1], 'k--')
      plt.xlim([0.0, 1.0]), plt.ylim([0.0, 1.05])
      plt.xlabel('False Positive Rate'), plt.ylabel('True Positive Rate')
      plt.title('ROC Curve'), plt.legend(loc="lower right")
      plt.show()
      return (roc_auc, fpr, tpr, thresholds)
    else:
      fig, axs = plt.subplots(1, 2, figsize=(12, 4))
      metric.plot_roc_curve(ax=axs[0])
      metric.plot_pr_curve(ax=axs[1])
      plt.show()
      return (roc_auc, fpr, tpr, thresholds)
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)