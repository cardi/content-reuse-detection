master_type=cc2.8xlarge
core_type=cc2.8xlarge
task_type=c1.xlarge

./elastic-mapreduce \
  --create --alive --log-uri s3://isi-emr-data/logs \
  --name "$core_type" \
  --enable-debugging \
  --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
  --args "-s,mapred.tasktracker.map.tasks.maximum=40,-s,mapred.tasktracker.reduce.tasks.maximum=0,-s,mapred.task.timeout=2000000" \
  --instance-group master --instance-type $master_type \
  --instance-count 1 --bid-price 1.00 \
  --instance-group core --instance-type $core_type \
  --instance-count 1 --bid-price 1.00 \
  --region us-east-1 \
  --availability-zone "us-east-1d"

# c1.xlarge -> 9 tasks <- 8 cores
#  --args "-s,mapred.tasktracker.map.tasks.maximum=9,-s,mapred.tasktracker.reduce.tasks.maximum=0,-s,mapred.task.timeout=2000000" \
#  --instance-group task --instance-type $task_type \
#  --instance-count 16 --bid-price 1.00  \

# cc2.8xlarge -> 2x eight core -> 16 threads each
# 16*20 -> 320 threads
# 24 mappers,b ut only 14 get used, 6 reducers, 0 needed
# 30 mappers
# 40 mappers each <-- utilization \approx 30?
