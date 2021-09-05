'''
Nombre de archivo:
  +main.py
Descripción: 
  +Archivo principal para ejecución del programa
'''

#librerías
from recursos import *
from preprocesamiento import *

#llamado a las funciones
#crear_df
#['/src/datasources/flights.csv', '/src/datasources/airports.csv', '/src/datasources/weather.csv']
df_list = crear_df(paths=['/src/datasources/flights.csv', '/src/datasources/airports.csv', '/src/datasources/weather.csv'],
          formats=['csv', 'csv', 'csv'], headers=[True, False, True], samples_fr=[1., 1., 1.], rand_st=999, print_=True)
#preprocesar_df
df_listready = preprocesar_df(df_list, print_=True)
#unir_df
df_jn = unir_df(df_listready, print_=True)
#ingeniería de características
df_ft = feating(df_jn, print_=True)

#operaciones en base de datos
print('Escribir en base de datos')
for df,table in zip(df_listready, ['tb_flights','tb_airports','tb_weather']):
  if escribir_df(df, table=table): print('Operación exitosa.')
if escribir_df(df_jn, table='tb_proyecto'): print('Operación exitosa.')
if escribir_df(df_ft[1], table='tb_proyectoml'): print('Operación exitosa.')
print('Leer desde base de datos')
df_db = leer_df(table='tb_proyecto')
df_db.show(10, truncate=False)
df_db.printSchema()