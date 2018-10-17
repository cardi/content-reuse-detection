#!/bin/bash

i=04
input=output-cc-DocVector-$i
output=output-cc-DocVector-$i-weirdurl

hadoop dfs -rm -r $output

python mr_cc_findweirdurl.py -r hadoop \
  --jobconf mapred.map.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.java.opts=-Xmx1450m \
  --jobconf mapred.child.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.tasks=0 \
  --jobconf mapred.job.name="mr_cc_findweirdurl-$output" \
  --no-output \
  -o hdfs:///user/calvin/$output \
  hdfs:///user/calvin/$input/part-*
