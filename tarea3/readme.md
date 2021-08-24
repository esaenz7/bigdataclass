# Instituto Tecnológico de Costa Rica

## Programa de Ciencia de los Datos - Módulo Big Data

### Tarea #3

* Esteban Sáenz Villalobos (**esaenz7@gmail.com**)
* Entrega: 29 de agosto 2021, 23:00.
* Observaciones: Ejecutar el programa siguiendo las instrucciones detalladas a continuación.

### Instrucciones

1. Para cargar el contenedor con todos los recursos necesarios, ejecute los archivos:

    a) clean_docker.sh. Este script borrará contenedores e imágenes antiguos correspondientes a este proyecto. **Atención: el comando realiza una acción de "prune" para la limpieza.**

    b) build_image.sh. Construye una imagen a partir del archivo DockerFile.

    c) run_image.sh. Este script creará 2 contenedores y una red local en docker de la siguiente forma:

        * Red: bigdatanet, IP: 10.7.84.0/24.
        * Host principal (sesión bash donde se ejecutarán los comandos): bigdata_tarea2_esv, IP: 10.7.84.101.
        * Host secundario (base de datos): postgres,  IP: 10.7.84.102.

2. Programa principal:

    a) Para ejecutar el programa principal se debe aplicar el siguiente comando:

        #spark-submit programaestudiante.py persona*.json

    b) Para ejecutar las pruebas del programa principal se debe aplicar el siguiente comando:

        #python -m pytest -vv test_programaestudiante.py

    c) Para ejecutar las instrucciones a y b anteriores de forma automática, ejecute el archivo:

        #run_main.sh.

### Detalles del trabajo

* El programa consta de los siguientes archivos:
    1. **procesamientodatos.py** (lógica de procesamiento).
    2. **programaestudiante.py** (programa principal).
    3. **conftest.py** (contexto para las pruebas).
    4. **test_programaestudiante.py** (ejecución de pruebas).
    5. **test_programaextra.py** (ejecución de pruebas del programa extra).
    6. **programaextra.py** (programa extra).
* La aplicación principal se ejecuta por etapas (stage) encapsuladas cada una por una función en específico.
    1. **Stage1**: cargar_datos(...).
    2. **Stage2**: generar_tablas(...).
    3. **Stage3**: almacenar_tablas(...).
* La fuente de datos del programa son archivos en formato JSON con el nombre "persona*.json" y con estructura definida según el enunciado del trabajo.
* El programa reconoce los nombres "fpersona*.json" como archivos tipo JSON con la columna adicional de fecha, por lo que el mismo código es capaz de ejecutar tanto estos archivos, como los archivos originales "persona*.json" (sin la columna fecha).
* El código fuente en cada archivo cuenta con comentarios detallados que explican la lógica del programa.
* Se inluye un jupyter notebook de Google Colab con todo el código necesario, como complemento.
* El repositorio completo de la tarea se encuentra también en el siguiente enlace [github/esaenz7](https://github.com/esaenz7/bigdataclass/tree/main/tarea3).

---
