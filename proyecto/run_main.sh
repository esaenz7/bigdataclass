#!/bin/bash
#
#data source
# gdown https://drive.google.com/uc?id=1c1nSkkEzzBqpcuxuZ3YnasmKT4GyMvOl
# gdown https://drive.google.com/uc?id=1gZTAbAtd1dePC-EDoruxp9uJUXsCAM58
# gdown https://drive.google.com/uc?id=14nu8FTLohAgZ17agET1DFRNccJN2LmFg
#
PGPASSWORD=testPassword psql -h 10.7.84.102 -U postgres -p 5432 < create_tables.sql
PGPASSWORD=testPassword psql -h 10.7.84.102 -U postgres -p 5432 < read_tables.sql
#
#main & test
# spark-submit main.py
# python -m pytest -vv
#
# jupyter notebook \
#     --ip=0.0.0.0 --port=8888 --allow-root
    # & --notebook-dir /host_data/  \
#