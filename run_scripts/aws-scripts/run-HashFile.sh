#!/bin/bash

./elastic-mapreduce --create \
  --name "test"  \
  --log-uri s3n://imi-emr-data/output \
  --debug \
  --num-instances 1 \
  --jar s3n://isi-emr-data/jar/isi+cc.jar \
  --arg isi.HashFile \
  --arg --hdfs \
  --arg --input \
  --arg s3n://aws-publicdatasets/common-crawl/crawl-002/2010/09/25/41/1285406207431_41.arc.gz \
  --arg --ouput \
  --arg s3n://isi-emr-data/output/output-cc-HashFile-03 \

