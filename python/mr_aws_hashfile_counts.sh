#!/bin/bash

i=02
o=02
input=output-aws-hashfile-$i
output=output-aws-hashfile-$o-counted

hadoop dfs -rm -r $output

python mr_cc_count_counts.py -r hadoop \
  --jobconf mapred.map.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.java.opts=-Xmx1450m \
  --jobconf mapred.child.java.opts=-Xmx1450m \
  --jobconf mapred.job.name="mr_aws_hashfile_counts-$i" \
  --no-output \
  -o hdfs:///user/calvin/$output \
  hdfs:///user/calvin/$input/part-*

#  --jobconf mapred.reduce.tasks=0 \
#--jobconf mapreduce.map.memory.mb=1536 --jobconf mapreduce.reduce.memory.mb=2048 
# --jobconf fs.inmemory.size.mb=200 \
# --jobconf io.file.buffer.size=131072 \
