#!/bin/bash
#
docker info
docker ps -a
docker stats --format "table {{.Name}}\t{{.CPUPerc}} {{.MemUsage}}\t{{.NetIO}}"
