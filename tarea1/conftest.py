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
#
#***resultados actuales*** obtenidos durante la ejecución del programa de forma regular
#
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
#
#***resultados actuales*** obtenidos durante la ejecución del programa alterando valores en los datos
#
#parámetro para prueba completa con valores en cero
@pytest.fixture
def tzerodata():
  dfzerodata = cargar_datos(['','ciclista.csv','ruta.csv','actividad.csv'])
  dfzerodata[1] = dfzerodata[1].withColumn('kms', F.when(dfzerodata[1]['kms'] < 40, 0).otherwise(dfzerodata[1]['kms'])) #se sustituyen algunos valores por 0 (valores menores a 40km se reemplazan por 0)
  rzerodata = presentar_datos(agregar_datos(unir_datos(dfzerodata, select=['fecha','nombre','provincia','nombre_ruta','kms']), group=['provincia','nombre'], agg='kms'), top=5, part='provincia', order='sum(kms)')
  return rzerodata
#parámetro para prueba completa con valores numéricos faltantes
@pytest.fixture
def tmissnumdata():
  dfmissnumdata = cargar_datos(['','ciclista.csv','ruta.csv','actividad.csv'])
  dfmissnumdata[1] = dfmissnumdata[1].withColumn('kms', F.when(dfmissnumdata[1]['kms'] < 40, '').otherwise(dfmissnumdata[1]['kms'])) #se sustituyen algunos valores numéricos por nulos (valores menores a 40km se reemplazan por valor nulo)
  rmissnumdata = presentar_datos(agregar_datos(unir_datos(dfmissnumdata, select=['fecha','nombre','provincia','nombre_ruta','kms']), group=['provincia','nombre'], agg='kms'), top=5, part='provincia', order='sum(kms)')
  return rmissnumdata
#parámetro para prueba completa con valores categóricos faltantes
@pytest.fixture
def tmisscatdata():
  dfmisscatdata = cargar_datos(['','ciclista.csv','ruta.csv','actividad.csv'])
  dfmisscatdata[0] = dfmisscatdata[0].withColumn('provincia', F.when(dfmisscatdata[0]['provincia'] == 'Alajuela', '').otherwise(dfmisscatdata[0]['provincia'])) #se sustituyen algunos valores categóricos por nulos (valores de columna provincia se reemplazan por valor nulo)
  rmisscatdata = presentar_datos(agregar_datos(unir_datos(dfmisscatdata, select=['fecha','nombre','provincia','nombre_ruta','kms']), group=['provincia','nombre'], agg='kms'), top=5, part='provincia', order='sum(kms)')
  return rmisscatdata

#***resultados esperados*** para pruebas de comparación
#
#parámetro para prueba de resultados finales
@pytest.fixture
def tstage4_expected():
  data = [('San José', 'FERNANDEZ ANDUJAR Ruben', 605.96, 43.28),
          ('San José', 'ALAPHILIPPE Julian', 348.77, 49.82),
          ('San José', 'ROJAS José Joaquin', 337.85, 37.54),
          ('San José', 'KÄMNA Lennard', 259.72, 51.94),
          ('San José', 'MOSCON Gianni', 253.49, 50.7),
          ('Heredia', 'CARAPAZ RICHARD ANTONIO', 434.51, 48.28),
          ('Heredia', 'PUCCIO Salvatore', 420.38, 38.22),
          ('Heredia', 'KELDERMAN Wilco', 390.18, 39.02),
          ('Heredia', 'MARCZYNSKI Tomasz', 361.53, 40.17),
          ('Heredia', 'NIELSEN Magnus Cort', 319.21, 45.6),
          ('Alajuela', 'WALLAYS Jelle', 517.33, 47.03),
          ('Alajuela', 'CHAVES RUBIO Johan Esteban', 413.35, 41.34),
          ('Alajuela', 'POLJANSKI Pawel', 355.0, 32.27),
          ('Alajuela', 'BARGUIL WARREN', 235.91, 39.32),
          ('Alajuela', 'ROCHE Nicolas', 207.31, 41.46),
          ('Limón', 'NIBALI Vincenzo', 239.48, 39.91),
          ('Limón', 'KONRAD Patrick', 205.88, 51.47),
          ('Limón', 'DE CLERCQ Bart', 204.79, 40.96),
          ('Limón', 'FROOME Christopher', 89.22, 44.61),
          ('Limón', 'MAJKA Rafal', 71.43, 35.72),
          ('Cartago', 'POELS Wout', 403.7, 40.37),
          ('Cartago', 'OSS Daniel', 379.12, 47.39),
          ('Cartago', 'BENEDETTI Cesare', 247.72, 41.29),
          ('Cartago', 'LAMPAERT Yves', 246.85, 41.14),
          ('Cartago', 'HAGA Chad', 163.24, 40.81),
          ('Guanacaste', 'YATES Simon', 440.34, 44.03),
          ('Guanacaste', 'KNEES Christian', 440.15, 48.91),
          ('Guanacaste', 'STANNARD Ian', 315.28, 45.04),
          ('Guanacaste', 'DENNIS Rohan', 241.37, 48.27),
          ('Puntarenas', 'BETANCUR GOMEZ Carlos Alberto', 460.15, 38.35),
          ('Puntarenas', 'HAIG Jack', 222.92, 55.73),
          ('Puntarenas', 'BUCHMANN Emanuel', 185.87, 37.17)]
  schema = StructType(\
                      [StructField('provincia',StringType()),
                      StructField('nombre',StringType()),
                      StructField('sum(kms)',DoubleType()),
                      StructField('avg(kms)',DoubleType()),])
  stage4_expected = spark.createDataFrame(data, schema)
  return stage4_expected
#parámetro para prueba completa con valores en cero
@pytest.fixture
def tzerodata_expected():
  data = [('San José', 'FERNANDEZ ANDUJAR Ruben', 428.93, 30.64),
          ('San José', 'ALAPHILIPPE Julian', 282.86, 40.41),
          ('San José', 'ROJAS José Joaquin', 240.15, 26.68),
          ('San José', 'PEDRERO Antonio', 231.82, 46.36),
          ('San José', 'SOLER GIMENEZ Marc', 196.04, 39.21),
          ('Puntarenas', 'BETANCUR GOMEZ Carlos Alberto', 356.21, 29.68),
          ('Puntarenas', 'HAIG Jack', 183.35, 45.84),
          ('Puntarenas', 'BUCHMANN Emanuel', 102.84, 20.57),
          ('Alajuela', 'WALLAYS Jelle', 445.13, 40.47),
          ('Alajuela', 'CHAVES RUBIO Johan Esteban', 308.93, 30.89),
          ('Alajuela', 'POLJANSKI Pawel', 254.64, 23.15),
          ('Alajuela', 'ROCHE Nicolas', 150.62, 30.12),
          ('Alajuela', 'BARGUIL WARREN', 107.76, 17.96),
          ('Limón', 'NIBALI Vincenzo', 206.37, 34.4),
          ('Limón', 'KONRAD Patrick', 205.88, 51.47),
          ('Limón', 'DE CLERCQ Bart', 149.23, 29.85),
          ('Limón', 'MAJKA Rafal', 53.88, 26.94),
          ('Limón', 'FROOME Christopher', 49.65, 24.83),
          ('Heredia', 'CARAPAZ RICHARD ANTONIO', 359.21, 39.91),
          ('Heredia', 'PUCCIO Salvatore', 335.96, 30.54),
          ('Heredia', 'KELDERMAN Wilco', 269.55, 26.96),
          ('Heredia', 'NIELSEN Magnus Cort', 230.63, 32.95),
          ('Heredia', 'MARCZYNSKI Tomasz', 230.34, 25.59),
          ('Cartago', 'POELS Wout', 351.48, 35.15),
          ('Cartago', 'OSS Daniel', 331.39, 41.42),
          ('Cartago', 'BENEDETTI Cesare', 183.35, 30.56),
          ('Cartago', 'HAGA Chad', 145.69, 36.42),
          ('Cartago', 'LAMPAERT Yves', 134.6, 22.43),
          ('Guanacaste', 'YATES Simon', 393.04, 39.3),
          ('Guanacaste', 'KNEES Christian', 386.87, 42.99),
          ('Guanacaste', 'STANNARD Ian', 282.17, 40.31),
          ('Guanacaste', 'DENNIS Rohan', 184.25, 36.85)]
  schema = StructType(\
                      [StructField('provincia',StringType()),
                      StructField('nombre',StringType()),
                      StructField('sum(kms)',DoubleType()),
                      StructField('avg(kms)',DoubleType()),])
  result = spark.createDataFrame(data, schema)
  return result
#parámetro para prueba completa con valores numéricos faltantes
@pytest.fixture
def tmissnumdata_expected():
  data = [('San José', 'FERNANDEZ ANDUJAR Ruben', 428.93, 53.62),
          ('San José', 'ALAPHILIPPE Julian', 282.86, 56.57),
          ('San José', 'ROJAS José Joaquin', 240.15, 60.04),
          ('San José', 'PEDRERO Antonio', 231.82, 57.96),
          ('San José', 'SOLER GIMENEZ Marc', 196.04, 49.01),
          ('Puntarenas', 'BETANCUR GOMEZ Carlos Alberto', 356.21, 50.89),
          ('Puntarenas', 'HAIG Jack', 183.35, 61.12),
          ('Puntarenas', 'BUCHMANN Emanuel', 102.84, 51.42),
          ('Alajuela', 'WALLAYS Jelle', 445.13, 55.64),
          ('Alajuela', 'CHAVES RUBIO Johan Esteban', 308.93, 51.49),
          ('Alajuela', 'POLJANSKI Pawel', 254.64, 50.93),
          ('Alajuela', 'ROCHE Nicolas', 150.62, 50.21),
          ('Alajuela', 'BARGUIL WARREN', 107.76, 53.88),
          ('Limón', 'NIBALI Vincenzo', 206.37, 51.59),
          ('Limón', 'KONRAD Patrick', 205.88, 51.47),
          ('Limón', 'DE CLERCQ Bart', 149.23, 49.74),
          ('Limón', 'MAJKA Rafal', 53.88, 53.88),
          ('Limón', 'FROOME Christopher', 49.65, 49.65),
          ('Heredia', 'CARAPAZ RICHARD ANTONIO', 359.21, 51.32),
          ('Heredia', 'PUCCIO Salvatore', 335.96, 55.99),
          ('Heredia', 'KELDERMAN Wilco', 269.55, 67.39),
          ('Heredia', 'NIELSEN Magnus Cort', 230.63, 57.66),
          ('Heredia', 'MARCZYNSKI Tomasz', 230.34, 57.59),
          ('Cartago', 'POELS Wout', 351.48, 50.21),
          ('Cartago', 'OSS Daniel', 331.39, 55.23),
          ('Cartago', 'BENEDETTI Cesare', 183.35, 61.12),
          ('Cartago', 'HAGA Chad', 145.69, 48.56),
          ('Cartago', 'LAMPAERT Yves', 134.6, 67.3),
          ('Guanacaste', 'YATES Simon', 393.04, 49.13),
          ('Guanacaste', 'KNEES Christian', 386.87, 55.27),
          ('Guanacaste', 'STANNARD Ian', 282.17, 56.43),
          ('Guanacaste', 'DENNIS Rohan', 184.25, 61.42)]
  schema = StructType(\
                      [StructField('provincia',StringType()),
                      StructField('nombre',StringType()),
                      StructField('sum(kms)',DoubleType()),
                      StructField('avg(kms)',DoubleType()),])
  result = spark.createDataFrame(data, schema)
  return result
#parámetro para prueba completa con valores categóricos faltantes
@pytest.fixture
def tmisscatdata_expected():
  data = [('San José', 'FERNANDEZ ANDUJAR Ruben', 605.96, 43.28),
          ('San José', 'ALAPHILIPPE Julian', 348.77, 49.82),
          ('San José', 'ROJAS José Joaquin', 337.85, 37.54),
          ('San José', 'KÄMNA Lennard', 259.72, 51.94),
          ('San José', 'MOSCON Gianni', 253.49, 50.7),
          ('Puntarenas', 'BETANCUR GOMEZ Carlos Alberto', 460.15, 38.35),
          ('Puntarenas', 'HAIG Jack', 222.92, 55.73),
          ('Puntarenas', 'BUCHMANN Emanuel', 185.87, 37.17),
          ('Limón', 'NIBALI Vincenzo', 239.48, 39.91),
          ('Limón', 'KONRAD Patrick', 205.88, 51.47),
          ('Limón', 'DE CLERCQ Bart', 204.79, 40.96),
          ('Limón', 'FROOME Christopher', 89.22, 44.61),
          ('Limón', 'MAJKA Rafal', 71.43, 35.72),
          ('Heredia', 'CARAPAZ RICHARD ANTONIO', 434.51, 48.28),
          ('Heredia', 'PUCCIO Salvatore', 420.38, 38.22),
          ('Heredia', 'KELDERMAN Wilco', 390.18, 39.02),
          ('Heredia', 'MARCZYNSKI Tomasz', 361.53, 40.17),
          ('Heredia', 'NIELSEN Magnus Cort', 319.21, 45.6),
          ('Cartago', 'POELS Wout', 403.7, 40.37),
          ('Cartago', 'OSS Daniel', 379.12, 47.39),
          ('Cartago', 'BENEDETTI Cesare', 247.72, 41.29),
          ('Cartago', 'LAMPAERT Yves', 246.85, 41.14),
          ('Cartago', 'HAGA Chad', 163.24, 40.81),
          ('', 'WALLAYS Jelle', 517.33, 47.03),
          ('', 'CHAVES RUBIO Johan Esteban', 413.35, 41.34),
          ('', 'POLJANSKI Pawel', 355.0, 32.27),
          ('', 'BARGUIL WARREN', 235.91, 39.32),
          ('', 'ROCHE Nicolas', 207.31, 41.46),
          ('Guanacaste', 'YATES Simon', 440.34, 44.03),
          ('Guanacaste', 'KNEES Christian', 440.15, 48.91),
          ('Guanacaste', 'STANNARD Ian', 315.28, 45.04),
          ('Guanacaste', 'DENNIS Rohan', 241.37, 48.27)]
  schema = StructType(\
                      [StructField('provincia',StringType()),
                      StructField('nombre',StringType()),
                      StructField('sum(kms)',DoubleType()),
                      StructField('avg(kms)',DoubleType()),])
  result = spark.createDataFrame(data, schema)
  return result