#!/bin/bash
#
jt -r
jt -t chesterish -T -N -cellw 100%
#
jupyter notebook stop 8888
jupyter notebook \
    --ip=0.0.0.0 --port=8888 --allow-root &
    # --notebook-dir /host_data/ \
#