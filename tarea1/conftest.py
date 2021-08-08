'''
Nombre de archivo:
  +conftest.py
Descripción: 
  +Archivo para definición del contexto para la ejecución de las pruebas
'''

#librerías necesarias
import pytest
from procesamientodatos import *

#sesión de spark
@pytest.fixture(scope="module")
def spark_session():
  spark = SparkSession.builder\
        .master("local")\
        .appName("Test#1")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  yield spark
  spark.stop()

#definición de los parámetros (fixtures) de pruebas según las diferentes etapas de ejecución del programa (stages)
#estos corresponden a los datos actuales obtenidos durante la ejecución del programa
#parámetro para prueba de carga de datos
stage1 = cargar_datos(['','ciclista.csv','ruta.csv','actividad.csv'])
@pytest.fixture
def tstage1():
  return stage1
#parámetro para prueba de unión de datos
stage2 = unir_datos(stage1, select=['fecha','nombre','provincia','nombre_ruta','kms'])
@pytest.fixture
def tstage2():
  return stage2
#parámetro para prueba de agregaciones parciales
stage3 = agregar_datos(stage2, group=['provincia','nombre'], agg='kms')
@pytest.fixture
def tstage3():
  return stage3
#parámetro para prueba de resultados finales
stage4 = presentar_datos(stage3, top=5, part='provincia', order='sum(kms)')
@pytest.fixture
def tstage4():
  return stage4
#parámetro para prueba de almacenamiento
stage5 = almacenar_datos(stage4, nombre='resultados.csv')
@pytest.fixture
def tstage5():
  return stage5
#parámetro para prueba completa con valores en cero
@pytest.fixture
def tcerodata():
  df = stage1
  df[1] = df[1].withColumn('kms', F.when(df[1]['kms'] < 40, 0).otherwise(df[1]['kms'])) #se sustituyen algunos valores por 0
  result = presentar_datos(agregar_datos(unir_datos(df, select=['fecha','nombre','provincia','nombre_ruta','kms']), group=['provincia','nombre'], agg='kms'), top=5, part='provincia', order='sum(kms)')
  return result
#parámetro para prueba completa con valores numéricos faltantes
@pytest.fixture
def tmissnumdata():
  df = stage1
  df[1] = df[1].withColumn('kms', F.when(df[1]['kms'] < 40, '').otherwise(df[1]['kms'])) #se sustituyen algunos valores numéricos por nulos
  result = presentar_datos(agregar_datos(unir_datos(df, select=['fecha','nombre','provincia','nombre_ruta','kms']), group=['provincia','nombre'], agg='kms'), top=5, part='provincia', order='sum(kms)')
  return result
#parámetro para prueba completa con valores categóricos faltantes
@pytest.fixture
def tmisscatdata():
  df = stage1
  df[0] = df[0].withColumn('provincia', F.when(df[0]['provincia'] == 'Alajuela', '').otherwise(df[0]['provincia'])) #se sustituyen algunos valores categóricos por nulos
  result = presentar_datos(agregar_datos(unir_datos(df, select=['fecha','nombre','provincia','nombre_ruta','kms']), group=['provincia','nombre'], agg='kms'), top=5, part='provincia', order='sum(kms)')
  return result