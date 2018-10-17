#!/bin/bash

#i=32
#input=output-geo-DocVector-$i
#j=32
#output=output-geo-DocVector-reduced-$i

i=04
input=output-cc-DocVector-$i
output=output-cc-DocVector-$i-reduced

hadoop dfs -rm -r $output

python mr_cc_vec_count.py -r hadoop \
  --jobconf mapred.map.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.java.opts=-Xmx1450m \
  --jobconf mapred.child.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.tasks=0 \
  --jobconf mapred.job.name="mr_cc_vec_counts-$output" \
  --no-output \
  -o hdfs:///user/calvin/$output \
  hdfs:///user/calvin/$input/part-*

#--jobconf mapreduce.map.memory.mb=1536 --jobconf mapreduce.reduce.memory.mb=2048 
# --jobconf fs.inmemory.size.mb=200 \
# --jobconf io.file.buffer.size=131072 \
