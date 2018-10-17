./elastic-mapreduce \
  -j $1 \
  --log-uri s3://isi-emr-data/output \
  --debug \
  --jar s3://isi-emr-data/jar/isi.jar \
  --main-class isi.HashFile \
  --arg s3://common-crawl/crawl-002/2010/09/25/41/ \
  --arg s3://isi-emr-data/output/output-cc-HashFile-09

# 492 files = 45.7783 GB
