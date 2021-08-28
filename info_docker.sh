#!/bin/bash
#
docker info
docker network ls
docker container ls
docker image ls
docker ps -a
docker stats --format "table {{.Name}}\t{{.CPUPerc}} {{.MemUsage}}\t{{.NetIO}}"
