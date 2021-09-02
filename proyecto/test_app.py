'''
Nombre de archivo:
  +test_app.py
Descripción: 
  +Archivo para ejecuión de pruebas del programa
'''

#librerías
import pytest
from recursos import *
from preprocesamiento import *

#pruebas según las diferentes etapas de ejecución del programa (stages)
#se comparan los valores actuales obtenidos desde los "fixtures" con los valores esperados según cada condición
#prueba de carga de datos
def test_stage1(tstage1):
  assert type(tstage1) == list
  assert len(tstage1) == 3
  assert tstage1[0].count() > 0
  assert tstage1[1].count() > 0
  assert tstage1[2].count() > 0
  assert str(tstage1[0].dtypes) == "[('FL_DATE', 'string'), ('OP_CARRIER', 'string'), ('OP_CARRIER_FL_NUM', 'int'), ('ORIGIN', 'string'), ('DEST', 'string'), ('CRS_DEP_TIME', 'int'), ('DEP_TIME', 'double'), ('DEP_DELAY', 'double'), ('TAXI_OUT', 'double'), ('WHEELS_OFF', 'double'), ('WHEELS_ON', 'double'), ('TAXI_IN', 'double'), ('CRS_ARR_TIME', 'int'), ('ARR_TIME', 'double'), ('ARR_DELAY', 'double'), ('CANCELLED', 'double'), ('CANCELLATION_CODE', 'string'), ('DIVERTED', 'double'), ('CRS_ELAPSED_TIME', 'double'), ('ACTUAL_ELAPSED_TIME', 'double'), ('AIR_TIME', 'double'), ('DISTANCE', 'double'), ('CARRIER_DELAY', 'double'), ('WEATHER_DELAY', 'double'), ('NAS_DELAY', 'double'), ('SECURITY_DELAY', 'double'), ('LATE_AIRCRAFT_DELAY', 'double'), ('Unnamed: 27', 'string')]"
  assert str(tstage1[1].dtypes) == "[('_c0', 'int'), ('_c1', 'string'), ('_c2', 'string'), ('_c3', 'string'), ('_c4', 'string'), ('_c5', 'string'), ('_c6', 'double'), ('_c7', 'double'), ('_c8', 'int'), ('_c9', 'string'), ('_c10', 'string'), ('_c11', 'string'), ('_c12', 'string'), ('_c13', 'string')]"
  assert str(tstage1[2].dtypes) == "[('EventId', 'string'), ('Type', 'string'), ('Severity', 'string'), ('StartTime(UTC)', 'string'), ('EndTime(UTC)', 'string'), ('TimeZone', 'string'), ('AirportCode', 'string'), ('LocationLat', 'double'), ('LocationLng', 'double'), ('City', 'string'), ('County', 'string'), ('State', 'string'), ('ZipCode', 'int')]"

#prueba de pre-procesamiento de datos
def test_stage2(tstage2):
  assert type(tstage2) == list
  assert len(tstage2) == 3
  assert tstage2[0].count() > 0
  assert tstage2[1].count() > 0
  assert tstage2[2].count() > 0
  assert str(tstage2[0].dtypes) == "[('date', 'date'), ('orig', 'string'), ('dest', 'string'), ('carrier', 'string'), ('sdeptim', 'int'), ('depdel', 'int'), ('txout', 'int'), ('sarrtim', 'int'), ('selap', 'int'), ('dist', 'int'), ('dyofwk', 'int'), ('wkofyr', 'int'), ('sdephr', 'int'), ('sarrhr', 'int'), ('label', 'int')]"
  assert str(tstage2[1].dtypes) == "[('iata', 'string'), ('icao', 'string')]"
  assert str(tstage2[2].dtypes) == "[('date', 'date'), ('wtyp', 'string'), ('wsev', 'string'), ('icao', 'string'), ('evhr', 'int'), ('evtim', 'int')]"

#prueba de unión de datos
def test_stage3(tstage3):
  assert type(tstage3) == DF
  assert tstage3.count() > 0
  assert str(tstage3.dtypes) == "[('carrier', 'string'), ('sdephr', 'int'), ('sarrhr', 'int'), ('dyofwk', 'int'), ('wkofyr', 'int'), ('wtyp', 'string'), ('wsev', 'string'), ('depdel', 'int'), ('txout', 'int'), ('selap', 'int'), ('dist', 'int'), ('evtim', 'int'), ('label', 'int')]"