./elastic-mapreduce \
  --create --alive --log-uri s3://isi-emr-data/output \
  --name "small" \
  --enable-debugging \
  --instance-count 1 \
  --region us-east-1
#  --instance-group master --instance-type m1.small \
#  --instance-count 1 --bid-price 1.00 \
#  --instance-group core --instance-type m1.small \
#  --instance-count 1 --bid-price 1.00 \
#  --region us-east-1
