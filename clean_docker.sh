#!/bin/bash
#
docker network rm bigdatanet
docker kill $(docker ps -q)
docker container prune -f
docker image prune -f
docker network prune -f
docker system prune -f
docker info
docker network ls
docker container ls
docker image ls
docker ps -a
