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
#['/src/datasources/flights.csv', '/src/datasources/airports.csv', '/src/datasources/weather.csv']
df_list = crear_df(paths=['flights.csv', 'airports.csv', 'weather.csv'],
          formats=['csv', 'csv', 'csv'], headers=[True, False, True], samples_fr=[.1, 1., .1], rand_st=999, print_=False)
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
#parámetro para prueba de ingeniería de características
df_ft = feating(df_jn, print_=False)
@pytest.fixture
def tstage4():
  return df_ft
# #parámetro para prueba
# @pytest.fixture
# def expected():
#   data = [('')]
#   df_data = spark.createDataFrame(data)
#   return df_data