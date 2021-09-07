#!/bin/bash
#
docker volume create host_data
#
docker network create \
    --driver=bridge \
    --subnet=10.7.84.0/24 \
    --ip-range=10.7.84.100/24 \
    --gateway=10.7.84.254 \
    bigdatanet
#
docker run --name bigdata_2 \
    --net bigdatanet --ip=10.7.84.102 \
    -e POSTGRES_PASSWORD=testPassword \
    -p 5433:5432 \
    -d postgres   
#
docker run --name bigdata_1 \
    --net bigdatanet --ip=10.7.84.101 \
    -p 8888:8888 \
    -v "$(pwd)":/host_data \
    -it bigdata \
    /bin/bash
#