#!/bin/sh
#
# This script starts the chi square calculation program on a cluster and writes the result to stdout
# also

/sw/venv/python39/dic24/bin/python run.py \
    --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar \
    -r hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json
