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
df_list = crear_df(paths=['2018.csv', 'airports.dat', 'WeatherEvents_Jan2016-Dec2020.csv'],
          formats=['csv', 'csv', 'csv'], headers=[True, False, True], samples_fr=[.001, 1., .001], rand_st=999, print_=True)
#preprocesar_df
df_listready = preprocesar_df(df_list, print_=True)
#unir_df
df_jn = unir_df(df_listready, print_=True)
# #almacenar en base de datos
# #conjuntos de datos individuales
# for df,table in zip(df_listready, ['flights','airports','weather']):
#   escribir_df(df, host, port, user, password, table)
# #conjunto de datos preparado
# escribir_df(df_jn, host, port, user, password, 'joined')
# #leer de base de datos
# df1 = leer_df(host, port, user, password, 'joined')