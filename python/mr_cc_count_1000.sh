#!/bin/bash

i=14
input=output-cc-FilterHtml-$i-counted
output=output-cc-FilterHtml-$i-counted-1000-interesting

hadoop dfs -rm -r $output

python mr_cc_count_1000.py -r hadoop \
  --jobconf mapred.map.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.java.opts=-Xmx1450m \
  --jobconf mapred.child.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.tasks=0 \
  --jobconf mapred.job.name="mr_cc_count_1000_py-$i" \
  --no-output \
  -o hdfs:///user/calvin/$output \
  hdfs:///user/calvin/$input/part-*

#--jobconf mapreduce.map.memory.mb=1536 --jobconf mapreduce.reduce.memory.mb=2048 
# --jobconf fs.inmemory.size.mb=200 \
# --jobconf io.file.buffer.size=131072 \
