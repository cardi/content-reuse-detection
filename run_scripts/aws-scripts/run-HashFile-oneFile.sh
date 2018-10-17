./elastic-mapreduce \
  -j $1 \
  --log-uri s3://isi-emr-data/output \
  --debug \
  --jar s3://isi-emr-data/jar/isi.jar \
  --main-class isi.HashFile \
  --arg s3://common-crawl/crawl-002/2010/09/25/41/1285406207431_41.arc.gz \
  --arg s3://isi-emr-data/output/output-cc-HashFile-9999
