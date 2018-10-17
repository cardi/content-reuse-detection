#!/bin/bash

i=03
hadoop fs -rm -r /user/calvin/output-aws-hashfile-$i

python mr_aws_hashfile.py -r hadoop \
  --jobconf mapred.job.name="mr_aws_hashfile-$i" \
  --jobconf mapred.output.compress=true \
  --jobconf mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
  --jobconf mapred.map.java.opts=-Xmx1450m \
  --jobconf mapred.reduce.java.opts=-Xmx1450m \
  --jobconf mapred.child.java.opts=-Xmx1450m \
  --jobconf mapreduce.map.maxattempts=30 \
  --jobconf mapreduce.reduce.maxattempte=30 \
  --jobconf mapreduce.job.maxtaskfailures.per.tracker=45 \
  --jobconf mapreduce.task.timeout=6000000 \
  --no-output \
  -o hdfs:///user/calvin/output-aws-hashfile-$i \
  hdfs:///user/calvin/output-aws-01/

# --jobconf mapred.map.java.opts=-Xmx1450m \
# --jobconf mapred.reduce.java.opts=-Xmx1450m \
# --jobconf mapred.child.java.opts=-Xmx1450m \
# --jobconf mapred.reduce.tasks=0 \

# --jobconf mapred.compress.map.output=true \
# --jobconf mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec \
