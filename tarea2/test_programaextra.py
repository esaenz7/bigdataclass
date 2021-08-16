'''
Nombre de archivo:
  +test_programaextra.py
Descripción: 
  +Archivo para la ejecuión de las pruebas del programa ***Parte Extra***
'''

#librerías necesarias
import pytest
from pyspark.sql import functions as F
from procesamientodatos import *

#definición de los parámetros (fixtures)
#
#***resultados actuales*** obtenidos durante la ejecución del programa de forma regular
#
#parámetro para prueba de carga de datos
dfinicial = cargar_datos('fpersona*.json') #datos correspondientes a los archivos con columna adicional de fecha (formato fpersona*.json)
@pytest.fixture
def tstage1():
  return dfinicial
#parámetro para prueba de procesamiento de datos
proceso = generar_tablas(dfinicial)
@pytest.fixture
def tstage2():
  return proceso
#parámetro para prueba de almacenamiento
archivos = almacenar_tablas(proceso, ['total_viajes.csv', 'total_ingresos.csv', 'metricas.csv', 'metricas_extra.csv'])
@pytest.fixture
def tstage3():
  return archivos
#
#***resultados actuales*** obtenidos durante la ejecución del programa alterando valores en los datos
#
#parámetro para prueba completa con valores en cero (kilometros)
@pytest.fixture
def tzerodata():
  dfzerodata = dfinicial[0]
  #se reemplazan algunos valores de la columna "kilometros" por 0
  dfzerodata = dfzerodata.withColumn('precio_kilometro', F.when(F.col('kilometros')<5, 0).otherwise(F.col('precio_kilometro')))
  #se procesan los datos con la función "generar_tablas()"
  rzerodata = generar_tablas([dfzerodata])
  return rzerodata[1]
#parámetro para prueba completa con valores nulos (codigo_postal_*)
@pytest.fixture
def tmissdata():
  dfmissdata = dfinicial[0]
  #se reemplazan algunos valores de las columnas "codigo_postal_destino" y "codigo_postal_destino" por nulos
  dfmissdata = dfmissdata.withColumn('codigo_postal_destino', F.when(F.col('kilometros')<5, '').otherwise(F.col('codigo_postal_destino')))
  dfmissdata = dfmissdata.withColumn('codigo_postal_origen', F.when(F.col('kilometros')>20, '').otherwise(F.col('codigo_postal_origen')))
  #se procesan los datos con la función "generar_tablas()"
  rmissdata = generar_tablas([dfmissdata])
  return rmissdata[0]
#parámetro para prueba completa con filas eliminadas (filtradas)
@pytest.fixture
def tdeldata():
  dfdeldata = dfinicial[0]
  #se eliminan algunas filas por completo (filtro por "precio_kilometro" > 450)
  dfdeldata = dfdeldata.filter('precio_kilometro > 450')
  #se procesan los datos con la función "generar_tablas()"
  rdeldata = generar_tablas([dfdeldata])
  return rdeldata[0]

#pruebas según las diferentes etapas de ejecución del programa (stages)
#se comparan los valores actuales obtenidos desde los "fixtures" con los valores esperados según cada condición
#
#prueba de carga de datos
def test_stage1(tstage1):
  assert type(tstage1) == list
  assert len(tstage1) == 1
  assert tstage1[0].count() == 50
  assert str(tstage1[0].dtypes) == "[('identificador', 'string'), ('codigo_postal_destino', 'string'), ('codigo_postal_origen', 'string'), ('fecha', 'string'), ('kilometros', 'string'), ('precio_kilometro', 'string')]"
#prueba de procesamiento de datos
def test_stage2(tstage2, tstage2_expected, tstageextra_expected):
  assert type(tstage2) == list
  assert tstage2[0].count() == 30
  assert tstage2[1].count() == 30
  assert str(tstage2[0].dtypes) == "[('codigo_postal', 'string'), ('tipo', 'string'), ('cantidad_viajes', 'bigint')]"
  assert str(tstage2[1].dtypes) == "[('codigo_postal', 'string'), ('tipo', 'string'), ('ingresos', 'double')]"
  #el método de comparación entre el dataframe esperado y el actual retorna la cantidad de filas distintas entre ambos, por su forma se implementa en ambas vías, actual vs esperado y esperado vs actual
  assert tstage2_expected[0].exceptAll(tstage2[0]).count() == 0
  assert tstage2[0].exceptAll(tstage2_expected[0]).count() == 0
  assert tstage2_expected[1].exceptAll(tstage2[1]).count() == 0
  assert tstage2[1].exceptAll(tstage2_expected[1]).count() == 0
  #es un método simple y funciona sin importar el orden de los datos, ejem. ordenando los datos de forma aleatoria:
  tstage2_shuffle = tstage2[1].orderBy(F.rand()) #df actual
  tstage2_expected_shuffle = tstage2_expected[1].orderBy(F.rand()) #df esperado
  assert tstage2_expected_shuffle.exceptAll(tstage2_shuffle).count() == 0
  assert tstage2_shuffle.exceptAll(tstage2_expected_shuffle).count() == 0  
  #prueba de tabla de métricas (archivos sin columna fecha)
  assert tstage2[2].count() == 7
  assert str(tstage2[2].dtypes) == "[('tipo_metrica', 'string'), ('valor', 'string')]"
  assert tstage2_expected[2].exceptAll(tstage2[2]).count() == 0
  assert tstage2[2].exceptAll(tstage2_expected[2]).count() == 0
  #***prueba EXTRA***
  #prueba de tabla de métricas funcionalidad extra (archivos con columna fecha)
  assert str(tstage2[3].dtypes) == "[('tipo_metrica', 'string'), ('fecha', 'string'), ('valor', 'string')]"
  assert tstageextra_expected[0].exceptAll(tstage2[3]).count() == 0
  assert tstage2[3].exceptAll(tstageextra_expected[0]).count() == 0
  #******************
#prueba de almacenamiento
def test_stage3(tstage3):
  assert type(tstage3) == list
  assert len(tstage3) == 4
  assert tstage3[0].count() == 30
  assert tstage3[1].count() == 30
  assert tstage3[2].count() == 7
#prueba completa con valores en cero
def test_zerodata(tzerodata, tzerodata_expected):
  assert tzerodata.count() == 30
  assert tzerodata_expected.exceptAll(tzerodata).count() == 0
  assert tzerodata.exceptAll(tzerodata_expected).count() == 0
#prueba completa con valores nulos
def test_missdata(tmissdata, tmissdata_expected):
  assert tmissdata.count() == 29
  assert tmissdata_expected.exceptAll(tmissdata).count() == 0
  assert tmissdata.exceptAll(tmissdata_expected).count() == 0
#prueba completa con filas eliminadas
def test_deldata(tdeldata, tdeldata_expected):
  tdeldata.show()
  tdeldata_expected.show()
  assert tdeldata.count() == 21
  assert tdeldata_expected.exceptAll(tdeldata).count() == 0
  assert tdeldata.exceptAll(tdeldata_expected).count() == 0
#