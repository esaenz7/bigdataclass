#!/bin/bash
#
PGPASSWORD=testPassword psql -h 10.7.84.102 -U postgres -p 5432 < create_tables.sql
#
jupyter notebook \
    --ip=0.0.0.0 --port=8888 --allow-root
    # & --notebook-dir /host_data/  \
#