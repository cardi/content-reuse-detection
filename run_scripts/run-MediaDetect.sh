#!/bin/bash

i=02
t=MediaDetect

hadoop dfs -rm -r /user/calvin/output-cc-$t-$i

hadoop jar isi.jar \
  isi.$t \
  /user/calvin/cc \
  /user/calvin/output-cc-$t-$i

#  /user/calvin/cc/1285406207431_41.arc.gz \
#  /user/calvin/cc \

#  -Dmapred.max.split.size=65536 \
#  -Dmapred.map.tasks=1600 \
#  -Dmapreduce.job.maps=1600 \
#  -libjars ArcInputFormat.jar,tika-app-1.1.jar \
