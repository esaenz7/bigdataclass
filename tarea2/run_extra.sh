#! /bin/bash
#
spark-submit programaestudiante.py fpersona*.json
python -m pytest -vv test_programaextra.py
#
PGPASSWORD=testPassword psql -h 10.7.84.102 -U postgres -p 5432 < create_metricas.sql
#
spark-submit \
    --driver-class-path postgresql-42.2.14.jar \
    --jars postgresql-42.2.14.jar \
    programaextra.py 10.7.84.102 5432 postgres testPassword metricas persona*.json
