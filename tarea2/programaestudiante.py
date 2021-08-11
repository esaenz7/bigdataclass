'''
Nombre de archivo:
  +programaestudiante.py
Descripción: 
  +Archivo principal (main) para la ejecución del programa
'''

#librerías necesarias
import sys, os
from procesamientodatos import *

#llamado a la función ejecutar_proceso
dfinicial = cargar_datos(files=sys.argv)
proceso = generar_tablas(dfinicial)
archivos = almacenar_tablas(proceso, ['total_viajes.csv', 'total_ingresos.csv', 'metricas.csv'])
#