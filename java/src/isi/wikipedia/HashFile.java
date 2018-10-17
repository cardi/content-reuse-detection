package isi.wikipedia;

import java.io.IOException;

import java.nio.charset.Charset;

import java.util.*;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;

import java.security.*;
import java.security.MessageDigest;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat;;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import isi.util.KMPMatch;
import isi.util.Utilities;

public class HashFile extends Configured implements Tool 
{
    public static class Map extends MapReduceBase 
      implements Mapper<BytesWritable, BytesWritable, Text, Text> 
    {

      private final static IntWritable one = new IntWritable(1);

      public void map(BytesWritable key,
                      BytesWritable value,
                      OutputCollector<Text, Text> collector, 
                      Reporter reporter) 
        throws IOException 
      {

        try {
          // status
          //reporter.setStatus("processing " + item.getArcFileName());
          reporter.getCounter("HashFile.counters", "Files Processed").increment(1);

          
          // get data
          // byte[] data = (value.toString()).getBytes(Charset.forName("UTF-8"));
          // http://docs.oracle.com/javase/6/docs/api/java/util/Arrays.html#copyOfRange%28byte%5b%5d,%20int,%20int%29
          //
          // when dealing with seq files, we need to ignore the first byte. why?
          byte[] data = Arrays.copyOfRange(value.getBytes(), 4, value.getLength()); // copy only valid bytes
          byte[] key2 = Arrays.copyOfRange(key.getBytes(), 4, key.getLength());

          // hash contents
          String hash = new String("");
          try {
            hash = Utilities.SHA256sum(data);
            reporter.getCounter("HashFile.counters", "Files Hashed").increment(1);
            collector.collect(new Text(hash), new Text(new String(key2)));
          } 
          catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            reporter.getCounter("HashFile.exception", e.getClass().getSimpleName()).increment(1);
          } //*/

          // debug:
          // collector.collect(new Text(key), new Text("1"));

        } // end try (process SEQ)
        catch(Exception e) {
          e.printStackTrace();
          reporter.getCounter("HashFile.exception", e.getClass().getSimpleName()).increment(1);
        } // end catch

      } // end public void map
    } // end public static class Map

    public static void main(String[] args) throws Exception 
    {
      // use this to automatically deal with -libjars option:
      // http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/util/ToolRunner.html
      ToolRunner.run(new HashFile(), args);
    }

    public int run(String[] args) throws Exception 
    {
      if (args.length != 2) 
      {
        throw new RuntimeException("usage: " + getClass().getName() + " <input> <output>");
      }
  
      JobConf conf = new JobConf(getConf(), getClass());
      Path output_path = new Path(args[1]);
      conf.setJobName(getClass().getName() + "-" + output_path.getName()); //set + output path

      // Job settings
      conf.setMaxMapTaskFailuresPercent(100);
      conf.setMaxMapAttempts(45);
      conf.setMemoryForMapTask(1400); // in MB

      conf.setNumReduceTasks(0);
      //conf.setMemoryForReduceTask(1400); // in MB

      conf.setInputFormat(SequenceFileAsBinaryInputFormat.class);
      //inputFormat.configure(conf);

      // Mapper Class
      conf.setMapperClass(Map.class);

      // Reducer Class (none for now)
      //conf.setReducerClass(Reduce.class);

      // Output
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      // run job
      JobClient.runJob(conf);

      return 0;
    }
}

//calvin@landercpu14:~$ hadoop fs -rmr /user/calvin/output-wp-hashfile-03Deleted hdfs://hcpu001.ant.isi.edu:6789/user/calvin/output-wp-hashfile-03
//calvin@landercpu14:~$ hadoop jar java-isi/build/isi-0.1.jar isi.wikipedia.HashFile /user/calvin/wikipedia-en-html-test.seq /user/calvin/output-wp-hashfile-03
