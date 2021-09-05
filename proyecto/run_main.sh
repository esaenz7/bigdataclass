#!/bin/bash
#
#data source
# gdown https://drive.google.com/uc?id=1fLPm7W7H6T_ziekLIRe4VY6a8QP7djDB
# gdown https://drive.google.com/uc?id=1Qhz8HpJ6zWvh73P81zJ1F4dXPK-LFyZ8
# gdown https://drive.google.com/uc?id=10xQyVp0z7PR39_PUHQnTY4QueF_OuzIl
#
#database
PGPASSWORD=testPassword psql -h 10.7.84.102 -U postgres -p 5432 < create_tables.sql
PGPASSWORD=testPassword psql -h 10.7.84.102 -U postgres -p 5432 < read_tables.sql
#
#main & test
spark-submit \
    --driver-class-path postgresql-42.2.14.jar \
    --jars postgresql-42.2.14.jar \
      main.py
python -m pytest -vv
#