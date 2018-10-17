package isi;

import java.io.IOException;

import java.util.*;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;

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
import org.apache.tika.Tika;

import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.HdfsARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;

import isi.util.KMPMatch;
import isi.util.Utilities;

public class ExtractURL extends Configured implements Tool 
{
    public static class Map extends MapReduceBase 
      implements Mapper<Text, ArcFileItem, Text, Text> 
      //implements Mapper<Text, ArcFileItem, Text, IntWritable> 
    {

      private final static IntWritable one = new IntWritable(1);

      public void map(Text url,
                      ArcFileItem item,
                      OutputCollector<Text, Text> collector, 
                      Reporter reporter) 
        throws IOException 
      {

        try {
          // status
          reporter.setStatus("processing " + item.getArcFileName());

          // initialize-- media detector
          Tika detector = new Tika();

          // get data
          byte[] data = item.getContent().getReadOnlyBytes(); // returns byte[]   
          String decoded_data = new String(data, "UTF-8"); // 20140123

          // output
          String arc_filename = item.getArcFileName();

          // check media via detector (prev: check file extension)
          try {
            String data_type = detector.detect(data);
    
            // TODO checking for dupes in URI
            // only process html/htm files
            Set<String> valid_filetypes  = new HashSet<String>();
            //valid_filetypes.addAll(Arrays.asList(
            //   "application/xml",
            //   "application/rss+xml",
            //   "text/plain",
            valid_filetypes.addAll(Arrays.asList(
               "application/xml",
               "application/rss+xml",
               "text/html",
               "application/xhtml+xml"
            ));
        
            // System.err.println("Processing " + url.toString());

            /* debug
            collector.collect(url, new Text("1")); //*/
           
            ///* 
            if(valid_filetypes.contains(data_type)) {
              reporter.getCounter("ExtractURL.counters", "Processed Files").increment(1);

              //System.err.println("Processing text");
              System.err.println("Processing text " + url.toString());

              // match
              // http://daringfireball.net/2010/07/improved_regex_for_matching_urls
              //Pattern p = Pattern.compile("(?i)\\b((?:https?://|www\\d{0,3}[.]|[a-z0-9.\\-]+[.][a-z]{2,4}/)(?:[^\\s()<>]+|\\(([^\\s()<>]+|(\\([^\\s()<>]+\\)))*\\))+(?:\\(([^\\s()<>]+|(\\([^\\s()<>]+\\)))*\\)|[^\\s`!()\\[\\]{};:'\".,<>?\u00AB\u00BB\u201C\u201D\u2018\u2019]))");
              Pattern p = Pattern.compile("(?i)\\b((?:https?://|www\\d{0,3}[.]|[a-z0-9.\\-]+[.][a-z]{2,4}/)(?:[^\\s()<>]+)+(?:[^\\s`!()\\[\\]{};:'\".,<>?\u00AB\u00BB\u201C\u201D\u2018\u2019]))");
              Matcher m = p.matcher(decoded_data);

              // regex matching 
              List<String> urls = new ArrayList<String>();
              while (m.find()) {
                urls.add(m.group());         
                reporter.getCounter("ExtractURL.counters", "urls found").increment(1);
              }
        
              //System.err.println("Building result");

              // build result
              String result = "";
              for (String u : urls) {
                if(result.equals("")) {
                  result = u;
                } else {
                  result = result + "  " + u; // XXX use tab or space delimiting? 
                  // for now use two spaces ("  ") 
                }
              }
        
              // collector collect
              //collector.collect(new Text(url.toString()), new Text(result));
              collector.collect(url, new Text(result)); // avoid unnecessary conversion

            } // end if(valid_filetypes)
            else {
              // do nothing
              reporter.getCounter("ExtractURL.counters", "Skipped Files").increment(1);
            } //*/
          }
          catch (IOException e) {
            e.printStackTrace();
            reporter.getCounter("ExtractURL.exception", e.getClass().getSimpleName()).increment(1);
          } // end try (media detection)
        } // end try (process ARC)
        catch(Exception e) {
          e.printStackTrace();
          reporter.getCounter("ExtractURL.exception", e.getClass().getSimpleName()).increment(1);
        } // end catch

      } // end public void map
    } // end public static class Map

/*
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
*/
    public static void main(String[] args) throws Exception 
    {
      // use this to automatically deal with -libjars option:
      // http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/util/ToolRunner.html
      ToolRunner.run(new ExtractURL(), args);
    }

    public int run(String[] args) throws Exception 
    {
      if (args.length != 2) 
      {
        throw new RuntimeException("usage: " + getClass().getName() + " <input> <output>");
      }
  
      JobConf conf = new JobConf(getConf(), getClass());
      conf.setJobName(getClass().getName());

      // Job settings
      conf.setMaxMapTaskFailuresPercent(100);
      conf.setMaxMapAttempts(45);
      conf.setMemoryForMapTask(1400); // in MB

      conf.setNumReduceTasks(0);
      //conf.setMemoryForReduceTask(1400); // in MB

      // set up ARCInputFormat options here
      conf.setInputFormat(ARCInputFormat.class);
      ARCInputFormat.setARCSourceClass(conf, HdfsARCSource.class);
      ARCInputFormat inputFormat = new ARCInputFormat();
      inputFormat.configure(conf);

      // Mapper Class
      conf.setMapperClass(Map.class);

      // Reducer Class (none for now)
      //conf.setReducerClass(Reduce.class);
      //
      // Compress Map output
      conf.set("mapred.output.compress","true");
      conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec");
      conf.set("mapred.compress.map.output","true");
      conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec");
      conf.set("mapred.job.priority", JobPriority.LOW.toString());

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
