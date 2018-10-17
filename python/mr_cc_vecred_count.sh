#!/bin/bash

i=04
input=output-cc-DocVector-$i-reduced
output=output-cc-DocVector-$i-reduced-hood

hadoop dfs -rm -r $output

python mr_cc_vecred_count.py -r hadoop \
  --jobconf mapred.map.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.java.opts=-Xmx1450m \
  --jobconf mapred.child.java.opts=-Xmx1450m \
  --no-output \
  -o hdfs:///user/calvin/$output \
  hdfs:///user/calvin/$input/part-*

#  --jobconf mapred.reduce.tasks=0 \
#--jobconf mapreduce.map.memory.mb=1536 --jobconf mapreduce.reduce.memory.mb=2048 
# --jobconf fs.inmemory.size.mb=200 \
# --jobconf io.file.buffer.size=131072 \
