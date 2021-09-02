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
df_list = crear_df(paths=['/src/datasources/2018.csv', '/src/datasources/airports.dat', '/src/datasources/WeatherEvents_Jan2016-Dec2020.csv'],
          formats=['csv', 'csv', 'csv'], headers=[True, False, True], samples_fr=[.0001, 1., .0001], rand_st=999, print_=True)
#preprocesar_df
df_listready = preprocesar_df(df_list, print_=True)
#unir_df
df_jn = unir_df(df_listready, print_=True)
#almacenar en base de datos
#conjuntos de datos individuales
# for df,table in zip(df_listready, ['flights','airports','weather']):
#   escribir_df(df, table=table)
#conjunto de datos preparado
# escribir_df(df_jn, table='tb_proyecto')
#leer de base de datos
# df1 = leer_df(table='tb_proyecto')