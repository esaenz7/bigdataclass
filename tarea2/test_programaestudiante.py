'''
Nombre de archivo:
  +test_programaestudiante.py
Descripción: 
  +Archivo para la ejecuión de las pruebas del programa
'''

#librerías necesarias
import pytest
from pyspark.sql import functions as F

#pruebas según las diferentes etapas de ejecución del programa (stages)
#se comparan los valores actuales obtenidos desde los "fixtures" con los valores esperados según cada condición
#
#prueba de carga de datos
def test_stage1(tstage1):
  assert type(tstage1) == list
  assert len(tstage1) == 1
  assert tstage1[0].count() == 50
  assert str(tstage1[0].dtypes) == "[('identificador', 'string'), ('codigo_postal_destino', 'string'), ('codigo_postal_origen', 'string'), ('kilometros', 'string'), ('precio_kilometro', 'string')]"
#prueba de procesamiento de datos
def test_stage2(tstage2, tstage2_expected):
  assert type(tstage2) == list
  assert len(tstage2) == 3
  assert tstage2[0].count() == 30
  assert tstage2[1].count() == 30
  assert tstage2[2].count() == 7
  assert str(tstage2[0].dtypes) == "[('codigo_postal', 'string'), ('tipo', 'string'), ('cantidad_viajes', 'bigint')]"
  assert str(tstage2[1].dtypes) == "[('codigo_postal', 'string'), ('tipo', 'string'), ('ingresos', 'double')]"
  assert str(tstage2[2].dtypes) == "[('tipo_metrica', 'string'), ('valor', 'string')]"
  #el método de comparación entre el dataframe esperado y el actual retorna la cantidad de filas distintas entre ambos, por su forma se implementa en ambas vías, actual vs esperado y esperado vs actual
  assert tstage2_expected[0].exceptAll(tstage2[0]).count() == 0
  assert tstage2[0].exceptAll(tstage2_expected[0]).count() == 0
  assert tstage2_expected[1].exceptAll(tstage2[1]).count() == 0
  assert tstage2[1].exceptAll(tstage2_expected[1]).count() == 0
  #es un método simple y funciona sin importar el orden de los datos, ejem. ordenando los datos de forma aleatoria:
  tstage2_shuffle = tstage2[2].orderBy(F.rand()) #df actual
  tstage2_expected_shuffle = tstage2_expected[2].orderBy(F.rand()) #df esperado
  assert tstage2_expected_shuffle.exceptAll(tstage2_shuffle).count() == 0
  assert tstage2_shuffle.exceptAll(tstage2_expected_shuffle).count() == 0  

#prueba de almacenamiento
def test_stage3(tstage3):
  assert type(tstage3) == list
  assert len(tstage3) == 3
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