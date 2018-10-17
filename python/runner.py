from mrjob.hadoop import HadoopJobRunner

#x = HadoopJobRunner(conf_path="/nfs/ruby/calvin/.mrjob", mr_job_script="mr_sha1.py", hadoop_input_format="org.apache.hadoop.mapred.SequenceFileAsTextInputFormat")
x = HadoopJobRunner(conf_path="/nfs/ruby/calvin/.mrjob", mr_job_script="mr_sha1.py", hadoop_input_format="org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat")
#x = HadoopJobRunner(hadoop_input_format="org.apache.hadoop.mapred.SequenceFileAsTextInputFormat")
x.run()


