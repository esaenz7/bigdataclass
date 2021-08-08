'''
Nombre de archivo:
  +test_programaestudiante.py
Descripción: 
  +Archivo para la ejecuión de las pruebas del programa
'''

#librerías necesarias
import pytest

#pruebas según las diferentes etapas de ejecución del programa (stages)
#se comparan los valores actuales obtenidos desde los "fixtures" con los valores esperados según cada condición
#prueba de carga de datos
def test_stage1(tstage1):
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
  assert type(tstage2) == list
  assert len(tstage2) == 2
  assert tstage2[0].count() == 300
  assert tstage2[1].count() == 300
  assert str(tstage2[0].dtypes) == "[('codigo_ruta', 'int'), ('cedula', 'int'), ('fecha', 'date'), ('codigo_ruta', 'int'), ('nombre_ruta', 'string'), ('kms', 'float'), ('cedula', 'int'), ('nombre', 'string'), ('provincia', 'string')]"
  assert str(tstage2[1].dtypes) == "[('fecha', 'date'), ('nombre', 'string'), ('provincia', 'string'), ('nombre_ruta', 'string'), ('kms', 'float')]"
#prueba de agregaciones parciales
def test_stage3(tstage3):
  assert type(tstage3) == list
  assert len(tstage3) == 1
  assert tstage3[0].count() == 47
  assert str(tstage3[0].dtypes) == "[('provincia', 'string'), ('nombre', 'string'), ('sum(kms)', 'double'), ('avg(kms)', 'double')]"
#prueba de resultados finales
def test_stage4(tstage4):
  assert type(tstage4) == list
  assert len(tstage4) == 1
  assert tstage4[0].count() == 32
  assert str(tstage4[0].dtypes) == "[('provincia', 'string'), ('nombre', 'string'), ('sum(kms)', 'double'), ('avg(kms)', 'double')]"
  assert tstage4[0].select('provincia').distinct().count() == 7
  assert str(tstage4[0].select('sum(kms)','avg(kms)').summary("count", "min", "max").collect()) == "[Row(summary='count', sum(kms)='32', avg(kms)='32'), Row(summary='min', sum(kms)='71.43', avg(kms)='32.27'), Row(summary='max', sum(kms)='605.96', avg(kms)='55.73')]"
#prueba de almacenamiento
def test_stage5(tstage5):
  assert type(tstage5) == list
  assert len(tstage5) == 1
  assert tstage5[0].count() == 32
#prueba completa con valores en cero
def test_cerodata(tcerodata):
  assert type(tcerodata) == list
  assert len(tcerodata) == 1
  assert tcerodata[0].count() == 32
  assert tcerodata[0].select('provincia').distinct().count() == 7
#prueba completa con valores numéricos faltantes
def test_missnumdata(tmissnumdata):
  assert type(tmissnumdata) == list
  assert len(tmissnumdata) == 1
  assert tmissnumdata[0].count() == 32
  assert tmissnumdata[0].select('provincia').distinct().count() == 7
#prueba completa con valores categóricos faltantes
def test_misscatdata(tmisscatdata):
  assert type(tmisscatdata) == list
  assert len(tmisscatdata) == 1
  assert tmisscatdata[0].count() == 32
  assert tmisscatdata[0].select('provincia').distinct().count() == 6