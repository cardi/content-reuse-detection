#!/bin/bash

i=31
input=output-geo-DocVector-$i
j=31
output=output-geo-DocVector-reduced-bloom-$i

hadoop dfs -rm -r $output

python mr_vec_bloom_count.py -r hadoop \
  --jobconf mapred.map.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.java.opts=-Xmx1450m \
  --jobconf mapred.child.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.tasks=0 \
  --file="w.bloomfilter.0100.correct" \
  --no-output \
  -o hdfs:///user/calvin/$output \
  hdfs:///user/calvin/$input/part-*

#--jobconf mapreduce.map.memory.mb=1536 --jobconf mapreduce.reduce.memory.mb=2048 
# --jobconf fs.inmemory.size.mb=200 \
# --jobconf io.file.buffer.size=131072 \
