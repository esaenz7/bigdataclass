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
#['/src/datasources/2018.csv', '/src/datasources/airports.dat', '/src/datasources/WeatherEvents_Jan2016-Dec2020.csv']
df_list = crear_df(paths=['/src/datasources/2018.csv', '/src/datasources/airports.dat', '/src/datasources/WeatherEvents_Jan2016-Dec2020.csv'],
          formats=['csv', 'csv', 'csv'], headers=[True, False, True], samples_fr=[.01, 1., .01], rand_st=999, print_=True)
#preprocesar_df
df_listready = preprocesar_df(df_list, print_=True)
#unir_df
df_jn = unir_df(df_listready, print_=True)
#operaciones en base de datos
print('Escribir en base de datos')
for df,table in zip(df_listready, ['tb_flights','tb_airports','tb_weather']):
  if escribir_df(df, table=table): print('Operación exitosa.')
if escribir_df(df_jn, table='tb_proyecto'): print('Operación exitosa.')
print('Leer desde base de datos')
df1 = leer_df(table='tb_proyecto')
df1.show(10, truncate=False)
df1.printSchema()