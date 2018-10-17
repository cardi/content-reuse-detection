package isi;

import java.io.IOException;

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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import org.apache.tika.parser.txt.CharsetDetector;
//import org.apache.tika.parser.txt.CharsetMatch;
import org.apache.tika.detect.MagicDetector;
import org.apache.tika.mime.MediaType;
import org.apache.tika.Tika;
import java.io.ByteArrayInputStream;

import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.HdfsARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;

import isi.util.KMPMatch;
import isi.util.Utilities;

public class MediaDetect extends Configured implements Tool 
{
    public static class Map extends MapReduceBase 
      implements Mapper<Text, ArcFileItem, Text, IntWritable> 
    {

      private final static IntWritable one = new IntWritable(1);

      public void map(Text url,
                      ArcFileItem item,
                      OutputCollector<Text, IntWritable> collector, 
                      Reporter reporter) 
        throws IOException 
      {

        try {
          // status
          reporter.setStatus("processing " + item.getArcFileName());

          // get data
          byte[] data = item.getContent().getReadOnlyBytes(); // returns byte[]
          //ByteArrayInputStream data_is = new ByteArrayInputStream(data);

          // output
          String arc_filename = item.getArcFileName();
        
          Tika detector = new Tika();

          // get media detect
          try {
            //MediaType data_type = MagicDetector.detect(data_is, null);
            String data_type = detector.detect(data);
            collector.collect(new Text(data_type), one);
          } 
          catch (IOException e) {
            e.printStackTrace();
            reporter.getCounter("MediaDetect.exception", e.getClass().getSimpleName()).increment(1);
          } // end try (media detection)

        } // end try (process ARC)
        catch(Exception e) {
          e.printStackTrace();
          reporter.getCounter("MediaDetect.exception", e.getClass().getSimpleName()).increment(1);
        } // end catch

      } // end public void map
    } // end public static class Map

    public static class Reduce extends MapReduceBase 
      implements Reducer<Text, IntWritable, Text, IntWritable> 
    {

      public void reduce(Text key,
                         Iterator<IntWritable> values, 
                         OutputCollector<Text, IntWritable> collector,
                         Reporter reporter) 
        throws IOException 
      {
        int sum = 0;
        while (values.hasNext()) {
          sum += values.next().get();
        }
        collector.collect(key, new IntWritable(sum));
      }
    }

    public static void main(String[] args) throws Exception 
    {
      // use this to automatically deal with -libjars option:
      // http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/util/ToolRunner.html
      ToolRunner.run(new MediaDetect(), args);
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

      Path input_path = new Path(args[0]);
      String input_path_s = input_path.getName();

      // check file extension
      int mid = input_path_s.lastIndexOf("."); // find position of /abcd/efgh/file.html
      String ext = input_path_s.substring(mid+1);

      if( ext.equals("gz") ) {
        conf.setNumReduceTasks(1);
      } else {
        conf.setMemoryForReduceTask(1400); // in MB
      }

      // set up ARCInputFormat options here
      conf.setInputFormat(ARCInputFormat.class);
      ARCInputFormat.setARCSourceClass(conf, HdfsARCSource.class);
      ARCInputFormat inputFormat = new ARCInputFormat();
      inputFormat.configure(conf);

      // Mapper Class
      conf.setMapperClass(Map.class);

      // Reducer Class
      conf.setReducerClass(Reduce.class);

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
