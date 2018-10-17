#!/bin/bash

i=01
input=output-aws-01
output=output-aws-reduced-$i

hadoop dfs -rm -r $output

python mr_aws_chunk_count.py -r hadoop \
  --jobconf mapred.map.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.java.opts=-Xmx1450m \
  --jobconf mapred.child.java.opts=-Xmx1450m \
  --jobconf mapred.job.name="mr_aws_chunk_count_py-$i" \
  --no-output \
  -o hdfs:///user/calvin/$output \
  hdfs:///user/calvin/$input

#--jobconf mapreduce.map.memory.mb=1536 --jobconf mapreduce.reduce.memory.mb=2048 
# --jobconf fs.inmemory.size.mb=200 \
# --jobconf io.file.buffer.size=131072 \
