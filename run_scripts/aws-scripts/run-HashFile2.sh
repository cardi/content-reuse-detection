./elastic-mapreduce \
  -j j-13G8U8BN1SPA6 \
  --log-uri s3://isi-emr-data/output \
  --debug \
  --jar s3://isi-emr-data/jar/isi-local.jar \
  --main-class isi.HashFile \
  --arg /input/41 \
  --arg s3://isi-emr-data/output/output-cc-HashFile-07
#  --arg common-crawl/crawl-002/2010/09/25/41/ \
#  --arg common-crawl/crawl-002/2010/09/25/41/1285406207431_41.arc.gz \
#  --arg s3://isi-emr-data/input/data.arc.gz \
#  --arg s3n://aws-publicdatasets/common-crawl/crawl-002/2010/09/25/41/1285406207431_41.arc.gz \
#  --arg /data.arc.gz \

#  https://forums.aws.amazon.com/thread.jspa?threadID=85827&tstart=0
