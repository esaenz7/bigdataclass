#!/bin/bash
#
docker run --name bigdata_tarea3_esv_2 \
    --net bigdatanet --ip=10.7.84.102 \
    -e POSTGRES_PASSWORD=testPassword \
    -p 5433:5432 \
    -d postgres   
#
docker run --name bigdata_tarea3_esv_1 \
    --net bigdatanet --ip=10.7.84.101 \
    -p 8888:8888 \
    -v "$(pwd)":/host_data \
    -t -d bigdata_tarea3_esv \
    /bin/bash
