#!/bin/bash
#
docker container stop bigdata_tarea2_esv_1
docker container stop bigdata_tarea2_esv_2
docker network rm bigdatanet
docker container prune -f
docker image prune -f
docker system prune -f
docker info
docker network ls
docker container ls
docker image ls
docker ps -a
