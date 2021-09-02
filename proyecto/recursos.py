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
from sklearn.metrics import roc_curve, auc, classification_report, confusion_matrix
from pyspark.sql import SparkSession, functions as F, window as W, DataFrame as DF
from pyspark.sql.types import (DateType, IntegerType, FloatType, DoubleType, LongType, StringType, StructField, StructType, TimestampType)
from pyspark.ml import functions as mlF, Pipeline as pipe
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import Imputer, StandardScaler, MinMaxScaler, Normalizer, PCA, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, DecisionTreeClassificationModel, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics
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
  .config("spark.jars", "postgresql-42.2.14.jar") \
  .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#funciones
#función para almacenar en base de datos
def escribir_df(df, host=host, port=port, user=user, password=password, table='table'):
  try:
    #almacenamiento en base de datos
      # .option("driver", "postgresql-42.2.14.jar") \
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
def leer_df(host=host, port=port, user=user, password=password, table='table'):
  try:
    #lectura desde base de datos hacia dataframe temporal
      # .option("driver", "postgresql-42.2.14.jar") \
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
def plot_metrics(dfcoll=None, ver=1, metric=None):
  try:
    fpr, tpr, thresholds = roc_curve(np.asarray(list(i[1] for i in dfcoll)), np.asarray(list(i[4][1] for i in dfcoll)))
    roc_auc = auc(fpr, tpr)
    conf_mat = confusion_matrix(list(i[1] for i in dfcoll), list(i[5] for i in dfcoll))
    if ver==1:
      fig,ax = plt.subplots(1,2, figsize=(12,4))
      ax[0].plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
      ax[0].plot([0, 1], [0, 1], 'k--')
      ax[0].set_xlim([0.0, 1.0]), ax[0].set_ylim([0.0, 1.05])
      ax[0].set_xlabel('Falsos positivos'), ax[0].set_ylabel('Verdaderos positivos')
      ax[0].set_title('Curva ROC'), ax[0].legend(loc="lower right")
      sns.heatmap(conf_mat, annot=True, fmt='.0f', ax=ax[1])
      ax[1].set_title('Matriz de confusión')
      plt.show()
    else:
      fig, axs = plt.subplots(1, 2, figsize=(12, 4))
      metric.plot_roc_curve(ax=axs[0])
      metric.plot_pr_curve(ax=axs[1])
      plt.show()
    return (roc_auc, fpr, tpr, thresholds, conf_mat)
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)