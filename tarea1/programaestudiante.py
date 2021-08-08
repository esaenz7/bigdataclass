'''
Nombre de archivo:
  +programaestudiante.py
Descripción: 
  +Archivo principal (main) para la ejecución del programa
'''

#librerías necesarias
import sys, os
from procesamientodatos import *

#llamado a las diferentes funciones para el procesamiento de los datos por etapas (stage#)
stage1 = cargar_datos(sys.argv)
stage2 = unir_datos(stage1, select=['fecha','nombre','provincia','nombre_ruta','kms'])
stage3 = agregar_datos(stage2, group=['provincia','nombre'], agg='kms')
stage4 = presentar_datos(stage3, top=5, part='provincia', order='sum(kms)', show=50)
stage5 = almacenar_datos(stage4, nombre='resultados.csv', show=50)
#