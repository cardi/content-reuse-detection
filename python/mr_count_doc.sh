#!/bin/bash

i=19

hadoop dfs -rm -r /user/calvin/output-output-geo-tdoc-$i

python mr_count_doc.py -r hadoop \
  --jobconf mapred.map.java.opts=-Xmx1024m \
  --jobconf mapred.reduce.java.opts=-Xmx1024m \
  --jobconf mapred.child.java.opts=-Xmx1024m \
  --no-output \
  -o hdfs:///user/calvin/output-output-geo-tdoc-$i \
  hdfs:///user/calvin/output-geo-t-$i/part-*

#--jobconf mapreduce.map.memory.mb=1536 --jobconf mapreduce.reduce.memory.mb=2048 
# --jobconf fs.inmemory.size.mb=200 \
# --jobconf io.file.buffer.size=131072 \
