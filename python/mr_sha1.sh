#!/bin/bash

hadoop dfs -rm -r /user/calvin/output-geocities

python mr_sha1.py -r hadoop \
  --jobconf mapred.child.java.opts=-Xmx1024m \
  --jobconf mapred.map.java.opts=-Xmx1024m \
  --jobconf mapred.reduce.java.opts=-Xmx1024m \
  --jobconf mapred.map.max.attempts=45 \
  --jobconf io.file.buffer.size=131072 \
  --jobconf mapred.skip.mode.enabled=true \
  --jobconf mapred.skip.map.max.skip.records=1000 \
  --jobconf mapred.skip.attempts.to.start.skipping=2 \
  --jobconf mapred.map.max.attempts=25 \
  --jobconf mapred.reduce.max.attempts=25 \
  --jobconf mapred.child.ulimit=3145728 \
  --jobconf dfs.namenode.handler.count=40 \
  --jobconf mapreduce.map.skip.maxrecords=1000 \
  --no-output \
  -o hdfs:///user/calvin/output-geocities \
  hdfs:///user/calvin/geocities

#  --quiet \
#  --jobconf mapreduce.job.maps=18000 \
#  --jobconf mapred.map.tasks=15000 \

#  --jobconf mapred.map.ulimit=1048576 \
#  --jobconf mapred.map.memory.mb=1024 \
#  --jobconf mapred.reduce.ulimit=1048576 \
#  --jobconf mapred.reduce.memory.mb=1024 \
#  --jobconf mapred.child.ulimit=1048576 \
#  --jobconf io.file.buffer.size=131072 \
#  --jobconf fs.inmemory.size.mb=200 \
#--jobconf mapreduce.map.memory.mb=1536 --jobconf mapreduce.reduce.memory.mb=2048 
