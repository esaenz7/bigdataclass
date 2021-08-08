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
  tstage1[0].show()
  tstage1[1].show()
  tstage1[2].show()
  assert type(tstage1) == list
  assert len(tstage1) == 3
  assert tstage1[0].count() == 50
  assert tstage1[1].count() == 15
  assert tstage1[2].count() == 300
  assert str(tstage1[0].dtypes) == "[('cedula', 'int'), ('nombre', 'string'), ('provincia', 'string')]"
  assert str(tstage1[1].dtypes) == "[('codigo_ruta', 'int'), ('nombre_ruta', 'string'), ('kms', 'float')]"
  assert str(tstage1[2].dtypes) == "[('codigo_ruta', 'int'), ('cedula', 'int'), ('fecha', 'date')]"
#prueba de unión de datos
def test_stage2(tstage2):
  tstage2[0].show()
  tstage2[1].show()
  assert type(tstage2) == list
  assert len(tstage2) == 2
  assert tstage2[0].count() == 300
  assert tstage2[1].count() == 300
  assert str(tstage2[0].dtypes) == "[('codigo_ruta', 'int'), ('cedula', 'int'), ('fecha', 'date'), ('codigo_ruta', 'int'), ('nombre_ruta', 'string'), ('kms', 'float'), ('cedula', 'int'), ('nombre', 'string'), ('provincia', 'string')]"
  assert str(tstage2[1].dtypes) == "[('fecha', 'date'), ('nombre', 'string'), ('provincia', 'string'), ('nombre_ruta', 'string'), ('kms', 'float')]"
#prueba de agregaciones parciales
def test_stage3(tstage3):
  tstage3[0].show()
  assert type(tstage3) == list
  assert len(tstage3) == 1
  assert tstage3[0].count() == 47
  assert str(tstage3[0].dtypes) == "[('provincia', 'string'), ('nombre', 'string'), ('sum(kms)', 'double'), ('avg(kms)', 'double')]"
#prueba de resultados finales
def test_stage4(tstage4, tstage4_expected):
  tstage4[0].show()
  tstage4_expected.show()
  assert type(tstage4) == list
  assert len(tstage4) == 1
  assert tstage4[0].count() == 32
  assert str(tstage4[0].dtypes) == "[('provincia', 'string'), ('nombre', 'string'), ('sum(kms)', 'double'), ('avg(kms)', 'double')]"
  assert tstage4[0].select('provincia').distinct().count() == 7
  assert str(tstage4[0].select('sum(kms)','avg(kms)').summary("count", "min", "max").collect()) == "[Row(summary='count', sum(kms)='32', avg(kms)='32'), Row(summary='min', sum(kms)='71.43', avg(kms)='32.27'), Row(summary='max', sum(kms)='605.96', avg(kms)='55.73')]"
  assert tstage4_expected.exceptAll(tstage4[0]).count() == 0
  assert tstage4[0].exceptAll(tstage4_expected).count() == 0
  #el método de comparación entre el dataframe esperado y el actual retorna la cantidad de filas distintas entre ambos, por su forma se implementa en ambas vías, actual vs esperado y esperado vs actual
  #es un método simple y funciona sin importar el orden de los datos, ejem. ordenando los datos de forma aleatoria:
  tstage4_shuffle = tstage4[0].orderBy(F.rand()) #df actual
  tstage4_expected_shuffle = tstage4_expected.orderBy(F.rand()) #df esperado
  assert tstage4_expected_shuffle.exceptAll(tstage4_shuffle).count() == 0
  assert tstage4_shuffle.exceptAll(tstage4_expected_shuffle).count() == 0
#prueba de almacenamiento
def test_stage5(tstage5):
  tstage5[0].show()
  assert type(tstage5) == list
  assert len(tstage5) == 1
  assert tstage5[0].count() == 32
#prueba completa con valores en cero
def test_zerodata(tzerodata, tzerodata_expected):
  tzerodata[0].show()
  tzerodata_expected.show()
  assert type(tzerodata) == list
  assert len(tzerodata) == 1
  assert tzerodata[0].count() == 32
  assert tzerodata[0].select('provincia').distinct().count() == 7
  assert tzerodata_expected.exceptAll(tzerodata[0]).count() == 0
  assert tzerodata[0].exceptAll(tzerodata_expected).count() == 0
#prueba completa con valores numéricos faltantes
def test_missnumdata(tmissnumdata, tmissnumdata_expected):
  tmissnumdata[0].show()
  tmissnumdata_expected.show()
  assert type(tmissnumdata) == list
  assert len(tmissnumdata) == 1
  assert tmissnumdata[0].count() == 32
  assert tmissnumdata[0].select('provincia').distinct().count() == 7
  assert tmissnumdata_expected.exceptAll(tmissnumdata[0]).count() == 0
  assert tmissnumdata[0].exceptAll(tmissnumdata_expected).count() == 0
#prueba completa con valores categóricos faltantes
def test_misscatdata(tmisscatdata, tmisscatdata_expected):
  tmisscatdata[0].show()
  tmisscatdata_expected.show()
  assert type(tmisscatdata) == list
  assert len(tmisscatdata) == 1
  assert tmisscatdata[0].count() == 32
  assert tmisscatdata[0].select('provincia').distinct().count() == 7
  assert tmisscatdata_expected.exceptAll(tmisscatdata[0]).count() == 0
  assert tmisscatdata[0].exceptAll(tmisscatdata_expected).count() == 0