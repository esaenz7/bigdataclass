'''
Nombre de archivo:
  +recursos.py
Descripción: 
  +Librería con recursos varios para la aplicación
'''

#librerías
from IPython.display import Javascript
import sys, os, glob, datetime as dt, numpy as np, random, collections as coll
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
from difflib import SequenceMatcher as seqmatch
import findspark
findspark.init('/usr/lib/python3.7/site-packages/pyspark')
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
def cols2vec(dfin, inputcols=[], outputcol='features', label='label', lab_alias='label', print_=False):
  try:
    assy = VectorAssembler(inputCols=inputcols, outputCol=outputcol, handleInvalid='skip')
    dfout = assy.transform(dfin)
    if lab_alias:
      dfout = dfout.select([outputcol, F.col(label).alias(lab_alias)])
    else:
      dfout = dfout.select([outputcol])
    if print_: dfout.show(10, truncate=False)
    return dfout
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función vector-columnas
def vec2cols(dfin, inputcol='features', outputcols=[], label='label', lab_alias='label', print_=False, prediction=None):
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
    if print_: dfout.show(10, truncate=False)
    return dfout
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función de graficación de correlaciones
def plot_corr(df=None, inputcols=[]):
  try:
    sns.set(font_scale=1.5)
    dfvec = cols2vec(df, inputcols=inputcols, outputcol='features')
    dfscaled = StandardScaler(inputCol='features', outputCol='scaled', withStd=True, withMean=True).fit(dfvec).transform(dfvec).select(['scaled', 'label'])
    pearson_matrix = Correlation.corr(dfscaled, column='scaled', method='pearson').collect()[0][0]
    dfcols = vec2cols(dfscaled, inputcol='scaled', outputcols=inputcols)
    print('\nMapa de calor')
    grid_kws = {"height_ratios":(1,.05), "hspace":.2}
    f,(ax,cbar_ax) = plt.subplots(2, gridspec_kw=grid_kws, figsize=(24,8))
    sns.heatmap(pearson_matrix.toArray(), yticklabels=inputcols, xticklabels=inputcols, mask=np.triu(pearson_matrix.toArray()),
                annot=True, fmt=".2f", linewidths=.5, cmap=sns.diverging_palette(220,20,as_cmap=True), ax=ax, cbar_ax=cbar_ax, cbar_kws={"orientation": "horizontal"})
    plt.show()
    print('\nGráfico de parcela')
    sns.pairplot(dfcols.toPandas(), height=2, aspect=16/9, corner=True, hue='label')
    plt.show()
    return dfscaled
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

#función de graficación ROC
def plot_metrics(dfcoll=None, ver=1, metric=None):
  try:
    sns.set(font_scale=1)
    fpr, tpr, thresholds = roc_curve(np.asarray(list(i[1] for i in dfcoll)), np.asarray(list(i[4][1] for i in dfcoll)))
    roc_auc = auc(fpr, tpr)
    conf_mat = confusion_matrix(list(i[1] for i in dfcoll), list(i[5] for i in dfcoll))
    if ver==1:
      fig,ax = plt.subplots(1,2, figsize=(12,4))
      ax[0].plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
      ax[0].plot([0, 1], [0, 1], 'k--')
      ax[0].set_xlim([-0.05, 1.0]), ax[0].set_ylim([0.0, 1.05])
      ax[0].set_xlabel('Falsos positivos'), ax[0].set_ylabel('Verdaderos positivos')
      ax[0].set_title('Curva ROC'), ax[0].legend(loc="lower right")
      sns.heatmap(conf_mat, annot=True, fmt='.0f', ax=ax[1])
      ax[1].set_title('Matriz de confusión')
      plt.show()
    else:
      fig, axs = plt.subplots(1, 2, figsize=(12,4))
      metric.plot_roc_curve(ax=axs[0])
      metric.plot_pr_curve(ax=axs[1])
      plt.show()
    return (roc_auc, fpr, tpr, thresholds, conf_mat)
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)

def plot_bound(trues, falses, n):
  try:
    fig,ax = plt.subplots(figsize=(12,4))
    ax.scatter(list(range(n)), trues[:n], s=10, alpha=0.7, c='r', marker="o", label='1')
    ax.scatter(list(range(n)), falses[:n], s=10, alpha=0.7, c='b', marker="s", label='0')
    plt.axhline(.5, color='green')
    plt.legend(loc='upper right'), ax.set_title('Límite de decisión')
    ax.set_xlabel('Observaciones'), ax.set_ylabel('Predicción de probabilidad')
    plt.show()
  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno, exc_obj)