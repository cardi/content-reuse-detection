./elastic-mapreduce \
  --create --alive --log-uri s3://isi-emr-data/logs \
  --name "$1" \
  --enable-debugging \
  --instance-group master --instance-type c1.xlarge \
  --instance-count 1 --bid-price 1.00 \
  --region us-east-1
#  --instance-group core --instance-type c1.xlarge \
#  --instance-count 4 --bid-price 1.00 

# --instance-group task --instance-type cc2.8xlarge \
# --instance-count 16 --bid-price 1.00  \
#  --availability-zone "us-west-1"
