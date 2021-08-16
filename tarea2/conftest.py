'''
Nombre de archivo:
  +conftest.py
Descripción: 
  +Archivo para definición del contexto para la ejecución de las pruebas
'''

#librerías necesarias
import pytest
from procesamientodatos import *

#definición de los parámetros (fixtures) con los resultados esperados para las pruebas de comparación
#
#parámetro para prueba de resultados finales completos
@pytest.fixture
def tstage2_expected():
  data1 = [('10101', 'origen', 6),
          ('10101', 'destino', 2),
          ('10201', 'origen', 4),
          ('10201', 'destino', 2),
          ('10301', 'origen', 2),
          ('10301', 'destino', 4),
          ('10601', 'destino', 2),
          ('10701', 'destino', 3),
          ('10801', 'destino', 1),
          ('11001', 'origen', 3),
          ('11101', 'destino', 6),
          ('11301', 'origen', 1),
          ('11301', 'destino', 3),
          ('11401', 'origen', 3),
          ('11501', 'origen', 3),
          ('11801', 'origen', 3),
          ('11801', 'destino', 4),
          ('20101', 'origen', 4),
          ('30101', 'origen', 5),
          ('30101', 'destino', 3),
          ('40101', 'destino', 2),
          ('40201', 'origen', 4),
          ('40301', 'origen', 5),
          ('40401', 'origen', 3),
          ('40401', 'destino', 5),
          ('40501', 'destino', 8),
          ('40701', 'origen', 3),
          ('40701', 'destino', 3),
          ('40801', 'origen', 1),
          ('40801', 'destino', 2)]
  data2 = [('10101', 'origen', 32703.0),
          ('10101', 'destino', 3276.0),
          ('10201', 'origen', 7055.0),
          ('10201', 'destino', 3645.0),
          ('10301', 'origen', 3492.5),
          ('10301', 'destino', 18571.0),
          ('10601', 'destino', 1920.0),
          ('10701', 'destino', 2545.5),
          ('10801', 'destino', 14670.0),
          ('11001', 'origen', 6115.0),
          ('11101', 'destino', 14734.0),
          ('11301', 'origen', 5322.0),
          ('11301', 'destino', 19721.0),
          ('11401', 'origen', 7395.0),
          ('11501', 'origen', 12216.0),
          ('11801', 'origen', 9708.0),
          ('11801', 'destino', 16446.5),
          ('20101', 'origen', 17651.5),
          ('30101', 'origen', 18368.5),
          ('30101', 'destino', 11412.0),
          ('40101', 'destino', 11944.0),
          ('40201', 'origen', 19432.0),
          ('40301', 'origen', 18010.5),
          ('40401', 'origen', 22559.5),
          ('40401', 'destino', 20627.0),
          ('40501', 'destino', 46159.0),
          ('40701', 'origen', 28654.0),
          ('40701', 'destino', 15537.0),
          ('40801', 'origen', 372.0),
          ('40801', 'destino', 7846.5)]
  data3 = [('persona_con_mas_kilometros', '01004'),
          ('persona_con_mas_ingresos', '01004'),
          ('percentil_25', '37148.5'),
          ('percentil_50', '38619.0'),
          ('percentil_75', '45756.0'),
          ('codigo_postal_origen_con_mas_ingresos', '10101'),
          ('codigo_postal_destino_con_mas_ingresos', '40501')]
  stage2_expected = [spark.createDataFrame(data1), spark.createDataFrame(data2), spark.createDataFrame(data3)]
  return stage2_expected
#parámetro para prueba de funcionalidad ***EXTRA*** (archivos con fecha)
@pytest.fixture
def tstageextra_expected():
  data1 = [('persona_con_mas_kilometros', '2020/06/11', '01004'),
          ('persona_con_mas_kilometros', '2020/06/10', '01004'),
          ('persona_con_mas_kilometros', '2020/06/09', '01002'),
          ('persona_con_mas_kilometros', '2020/06/08', '01004'),
          ('persona_con_mas_kilometros', '2020/06/07', '01002'),
          ('persona_con_mas_ingresos', '2020/06/11', '01004'),
          ('persona_con_mas_ingresos', '2020/06/10', '01004'),
          ('persona_con_mas_ingresos', '2020/06/09', '01002'),
          ('persona_con_mas_ingresos', '2020/06/08', '01004'),
          ('persona_con_mas_ingresos', '2020/06/07', '01002'),
          ('percentil_25', '2020/06/11', '3125.5'),
          ('percentil_25', '2020/06/10', '5475.0'),
          ('percentil_25', '2020/06/09', '4880.5'),
          ('percentil_25', '2020/06/08', '6135.0'),
          ('percentil_25', '2020/06/07', '7919.0'),
          ('percentil_50', '2020/06/11', '5562.0'),
          ('percentil_50', '2020/06/10', '5679.0'),
          ('percentil_50', '2020/06/09', '6183.0'),
          ('percentil_50', '2020/06/08', '12618.0'),
          ('percentil_50', '2020/06/07', '13450.5'),
          ('percentil_75', '2020/06/11', '6411.0'),
          ('percentil_75', '2020/06/10', '6604.5'),
          ('percentil_75', '2020/06/09', '8656.0'),
          ('percentil_75', '2020/06/08', '14166.5'),
          ('percentil_75', '2020/06/07', '13495.0'),
          ('codigo_postal_origen_con_mas_ingresos', '2020/06/11', '20101'),
          ('codigo_postal_origen_con_mas_ingresos', '2020/06/10', '30101'),
          ('codigo_postal_origen_con_mas_ingresos', '2020/06/09', '40301'),
          ('codigo_postal_origen_con_mas_ingresos', '2020/06/08', '10101'),
          ('codigo_postal_origen_con_mas_ingresos', '2020/06/07', '40401'),
          ('codigo_postal_destino_con_mas_ingresos', '2020/06/11', '40501'),
          ('codigo_postal_destino_con_mas_ingresos', '2020/06/10', '40501'),
          ('codigo_postal_destino_con_mas_ingresos', '2020/06/09', '40401'),
          ('codigo_postal_destino_con_mas_ingresos', '2020/06/08', '10801'),
          ('codigo_postal_destino_con_mas_ingresos', '2020/06/07', '40701')]
  stageextra_expected = [spark.createDataFrame(data1)]
  return stageextra_expected
#parámetro para prueba completa con valores en cero
@pytest.fixture
def tzerodata_expected():
  data = [('10101', 'origen', 30489.0),
          ('10101', 'destino', 0.0),
          ('10201', 'origen', 5374.0),
          ('10201', 'destino', 2934.0),
          ('10301', 'origen', 2628.0),
          ('10301', 'destino', 17630.5),
          ('10601', 'destino', 0.0),
          ('10701', 'destino', 0.0),
          ('10801', 'destino', 14670.0),
          ('11001', 'origen', 4655.0),
          ('11101', 'destino', 13231.5),
          ('11301', 'origen', 5322.0),
          ('11301', 'destino', 17130.5),
          ('11401', 'origen', 6075.0),
          ('11501', 'origen', 8563.5),
          ('11801', 'origen', 8997.0),
          ('11801', 'destino', 13371.5),
          ('20101', 'origen', 16711.0),
          ('30101', 'origen', 16173.0),
          ('30101', 'destino', 11412.0),
          ('40101', 'destino', 11944.0),
          ('40201', 'origen', 19432.0),
          ('40301', 'origen', 15796.5),
          ('40401', 'origen', 22559.5),
          ('40401', 'destino', 19562.0),
          ('40501', 'destino', 46159.0),
          ('40701', 'origen', 27793.0),
          ('40701', 'destino', 14677.0),
          ('40801', 'origen', 0.0),
          ('40801', 'destino', 7846.5)]
  schema = None
  result = spark.createDataFrame(data, schema)
  return result
#parámetro para prueba completa con valores nulos
@pytest.fixture
def tmissdata_expected():
  data = [('', 'origen', 5),
          ('', 'destino', 16),
          ('10101', 'origen', 5),
          ('10201', 'origen', 4),
          ('10201', 'destino', 1),
          ('10301', 'origen', 2),
          ('10301', 'destino', 3),
          ('10801', 'destino', 1),
          ('11001', 'origen', 3),
          ('11101', 'destino', 4),
          ('11301', 'origen', 1),
          ('11301', 'destino', 2),
          ('11401', 'origen', 3),
          ('11501', 'origen', 3),
          ('11801', 'origen', 3),
          ('11801', 'destino', 2),
          ('20101', 'origen', 4),
          ('30101', 'origen', 4),
          ('30101', 'destino', 3),
          ('40101', 'destino', 2),
          ('40201', 'origen', 4),
          ('40301', 'origen', 5),
          ('40401', 'origen', 2),
          ('40401', 'destino', 4),
          ('40501', 'destino', 8),
          ('40701', 'origen', 1),
          ('40701', 'destino', 2),
          ('40801', 'origen', 1),
          ('40801', 'destino', 2)]
  schema = None
  result = spark.createDataFrame(data, schema)
  return result
#parámetro para prueba completa con filas eliminadas
@pytest.fixture
def tdeldata_expected():
  data = [('10101', 'origen', 2),
          ('10101', 'destino', 1),
          ('10201', 'origen', 1),
          ('10301', 'destino', 2),
          ('10701', 'destino', 1),
          ('10801', 'destino', 1),
          ('11001', 'origen', 2),
          ('11101', 'destino', 1),
          ('11301', 'destino', 1),
          ('11401', 'origen', 1),
          ('11501', 'origen', 2),
          ('11801', 'origen', 1),
          ('11801', 'destino', 2),
          ('20101', 'origen', 2),
          ('30101', 'destino', 2),
          ('40201', 'origen', 1),
          ('40301', 'origen', 1),
          ('40401', 'origen', 1),
          ('40501', 'destino', 3),
          ('40701', 'origen', 2),
          ('40701', 'destino', 2)]
  schema = None
  result = spark.createDataFrame(data, schema)
  return result
#