#!/bin/bash
#
jt -r
jt -t gruvboxd -fs 9 -nfs 9 -tfs 11 -dfs 9 -ofs 9 -cellw 100% -T -N -kl
# onedork
#
jupyter notebook stop 8888
jupyter notebook \
    --ip=0.0.0.0 --port=8888 --allow-root &
    # --notebook-dir /host_data/ \
#