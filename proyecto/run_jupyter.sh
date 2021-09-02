#!/bin/bash
#
jupyter notebook \
    --notebook-dir /host_data/  \
    --ip=0.0.0.0 --port=8888 --allow-root
#