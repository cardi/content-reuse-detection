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
import org.apache.tika.Tika;

import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.HdfsARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;

import isi.util.KMPMatch;
import isi.util.Utilities;

public class FilterHtml extends Configured implements Tool 
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

          // output
          String arc_filename = item.getArcFileName();

          // check media via detector (prev: check file extension)
          try {
            String data_type = detector.detect(data);
    
            // TODO checking for dupes in URI
            // only process html/htm files
            Set<String> valid_filetypes  = new HashSet<String>();
            valid_filetypes.addAll(Arrays.asList(
               "application/xml",
               "application/rss+xml",
               "text/plain",
               "text/html",
               "application/xhtml+xml"
            ));

            if(valid_filetypes.contains(data_type)) {
              reporter.getCounter("FilterHtml.counters", "Processed Files").increment(1);
         
              /* match */
              ArrayList<Integer> indexes =  KMPMatch.allIndexOf(data, "<P ".getBytes("UTF8"));
              ArrayList<Integer> indexes2 = KMPMatch.allIndexOf(data, "<P>".getBytes("UTF8"));
              ArrayList<Integer> indexes3 = KMPMatch.allIndexOf(data, "<P\t".getBytes("UTF8"));
              ArrayList<Integer> indexes4 = KMPMatch.allIndexOf(data, "<P/".getBytes("UTF8"));
              ArrayList<Integer> indexes5 = KMPMatch.allIndexOf(data, "<p ".getBytes("UTF8"));
              ArrayList<Integer> indexes6 = KMPMatch.allIndexOf(data, "<p>".getBytes("UTF8"));
              ArrayList<Integer> indexes7 = KMPMatch.allIndexOf(data, "<p\t".getBytes("UTF8"));
              ArrayList<Integer> indexes8 = KMPMatch.allIndexOf(data, "<p/".getBytes("UTF8"));

              reporter.getCounter("FilterHtml.counters",      "<P").increment(indexes.size());
              reporter.getCounter("FilterHtml.counters",     "<P>").increment(indexes2.size());
              reporter.getCounter("FilterHtml.counters", "<P(tab)").increment(indexes3.size());
              reporter.getCounter("FilterHtml.counters",     "<P/").increment(indexes4.size());
              reporter.getCounter("FilterHtml.counters",      "<p").increment(indexes5.size());
              reporter.getCounter("FilterHtml.counters",     "<p>").increment(indexes6.size());
              reporter.getCounter("FilterHtml.counters", "<p(tab)").increment(indexes7.size());
              reporter.getCounter("FilterHtml.counters",     "<p/").increment(indexes8.size());
  
              indexes.addAll(indexes2);
              indexes.addAll(indexes3);
              indexes.addAll(indexes4);
              indexes.addAll(indexes5);
              indexes.addAll(indexes6);
              indexes.addAll(indexes7);
              indexes.addAll(indexes8);
              Collections.sort(indexes);
  
              // so we always start from the beginning of file
              if(!indexes.contains(new Integer(0))) {
                indexes.add(0, new Integer(0)); // first element
              }
              
              // slightly inaccurate because we consider from the start of the document
              reporter.getCounter("FilterHtml.counters", "Total Paragraph Chunks").increment(indexes.size());
  
              byte[] subArray;
              int x_len = data.length; // for byte arrays
  
              // get array slices
              for(int i=0; i<indexes.size(); i++) {
                int b = indexes.get(i);
                int end;
                if(i != indexes.size()-1) {
                  end = indexes.get(i+1);
                  //System.out.println("Index: " + indexes.get(i) + "," + indexes.get(i+1));
                }
                else {
                  end = x_len;
                  //System.out.println("Index: " + indexes.get(i) + "," + x_len);
                }
  
                //System.out.println(x.substring(b,e));
                try {
                  //System.out.println("Index: " + b + ":" + end);
                  subArray = Arrays.copyOfRange(data, b, end);
  
                  // hash contents
                  String hash = new String("");
                  try {
                    hash = Utilities.SHAsum(subArray);
                  } 
                  catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                    reporter.getCounter("FilterHtml.exception", e.getClass().getSimpleName()).increment(1);
                  }
                
                  collector.collect(new Text(hash), new Text(arc_filename + ":" + b + ":" + url.toString() ));
                } //end try 
                catch (Exception e) 
                {
                  e.printStackTrace();
                  reporter.getCounter("FilterHtml.exception", e.getClass().getSimpleName()).increment(1);
                }
  
              } // end for loop
            } // end if(valid_filetypes)
            else {
              // do nothing
              reporter.getCounter("FilterHtml.counters", "Skipped Files").increment(1);
            }
          }
          catch (IOException e) {
            e.printStackTrace();
            reporter.getCounter("FilterHtml.exception", e.getClass().getSimpleName()).increment(1);
          } // end try (media detection)
        } // end try (process ARC)
        catch(Exception e) {
          e.printStackTrace();
          reporter.getCounter("FilterHtml.exception", e.getClass().getSimpleName()).increment(1);
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
      ToolRunner.run(new FilterHtml(), args);
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
