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
df_list = crear_df(paths=['2018.csv', 'airports.dat', 'WeatherEvents_Jan2016-Dec2020.csv'],
          formats=['csv', 'csv', 'csv'], headers=[True, False, True], samples_fr=[.001, 1., .001], rand_st=999, print_=True)
#preprocesar_df
df_listready = preprocesar_df(df_list, print_=True)
#unir_df
df_jn = unir_df(df_listready, print_=True)
# #operaciones en base de datos
# print('Escribir en base de datos')
# for df,table in zip(df_listready, ['flights','airports','weather']):
#   escribir_df(df, table=table)
# escribir_df(df_jn, table='tb_proyecto')
# print('Leer desde base de datos')
# df1 = leer_df(table='tb_proyecto')
# df1.show(10, truncate=False)
# df1.printSchema()