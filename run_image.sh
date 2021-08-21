#!/bin/bash
docker run \
    --name bigdata \
    -p 8888:8888 \
    -v "$(pwd)":/host_data \
    -it bigdata /bin/bash

docker run \
    --name bigdata-db \
    -p 5433:5432 \
    -e POSTGRES_PASSWORD=testPassword \
    -d postgres

