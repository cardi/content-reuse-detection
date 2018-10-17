#!/bin/bash

i=15
hadoop dfs -rm -r /user/calvin/output-cc-FilterHtml-$i-counted

python mr_cc.py -r hadoop \
  --jobconf mapred.map.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.java.opts=-Xmx1450m \
  --jobconf mapred.child.java.opts=-Xmx1450m \
  --jobconf mapred.job.name="mr_cc_counts-$i" \
  --no-output \
  -o hdfs:///user/calvin/output-cc-FilterHtml-$i-counted\
  hdfs:///user/calvin/output-cc-FilterHtml-$i/

#  --jobconf mapred.reduce.tasks=0 \
