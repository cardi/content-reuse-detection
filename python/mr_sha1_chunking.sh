#!/bin/bash

python mr_sha1_chunking.py -r hadoop \
  --jobconf mapred.child.java.opts=-Xmx1400m \
  --jobconf mapred.map.max.attempts=25 \
  --jobconf mapred.child.ulimit=3145728 \
  --jobconf io.file.buffer.size=131072 \
  --jobconf mapreduce.job.maps=12000 \
  --no-output \
  -o hdfs:///user/calvin/output-geocities-chunk \
  hdfs:///user/calvin/geocities

# 4000 ~= 100gb
