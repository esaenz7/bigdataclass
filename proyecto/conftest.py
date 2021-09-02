'''
Nombre de archivo:
  +conftest.py
Descripción: 
  +Archivo para definición del contexto para ejecución de pruebas
'''

#librerías
import pytest
from recursos import *
from preprocesamiento import *

#definición de los parámetros (fixtures)
#parámetro para prueba de carga de datos
#['/src/datasources/2018.csv', '/src/datasources/airports.dat', '/src/datasources/WeatherEvents_Jan2016-Dec2020.csv']
df_list = crear_df(paths=['/src/datasources/2018.csv', '/src/datasources/airports.dat', '/src/datasources/WeatherEvents_Jan2016-Dec2020.csv'],
          formats=['csv', 'csv', 'csv'], headers=[True, False, True], samples_fr=[.0001, 1., .0001], rand_st=999, print_=False)
@pytest.fixture
def tstage1():
  return df_list
#parámetro para prueba de pre-procesamiento de datos
df_listready = preprocesar_df(df_list, print_=False)
@pytest.fixture
def tstage2():
  return df_listready
#parámetro para prueba de unión de datos
df_jn = unir_df(df_listready, print_=False)
@pytest.fixture
def tstage3():
  return df_jn
# #parámetro para prueba
# @pytest.fixture
# def expected():
#   #descripción estadística esperada
#   data = [('')]
#   df_data = spark.createDataFrame(data)
#   return df_data