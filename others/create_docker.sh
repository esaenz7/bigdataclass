#!/bin/bash
#
docker volume create host_data

docker network create \
    --driver=bridge \
    --subnet=10.7.84.0/24 \
    --ip-range=10.7.84.100/24 \
    --gateway=10.7.84.254 \
    bigdatanet