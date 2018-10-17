./elastic-mapreduce \
  -j $1 \
  --log-uri s3://isi-emr-data/output \
  --debug \
  --jar s3://isi-emr-data/jar/isi.jar \
  --main-class isi.FilterHtml \
  --arg s3://common-crawl/crawl-002/ \
  --arg s3://isi-emr-data/output/output-cc-FilterHtml-04
