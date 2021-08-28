#!/bin/bash
#
docker info
docker network ls
docker network inspect bigdatanet |grep -e IP -e Name
docker image ls
docker container ls
docker ps -a
docker stats --format "table {{.Name}}\t{{.CPUPerc}} {{.MemUsage}}\t{{.NetIO}}"
