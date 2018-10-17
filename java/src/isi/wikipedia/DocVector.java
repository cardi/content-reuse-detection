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

import org.apache.tika.Tika; // for file detection

public class DocVector extends Configured implements Tool 
{
    public static class Map extends MapReduceBase 
      implements Mapper<BytesWritable, BytesWritable, Text, Text> 
    {

      private final static IntWritable one = new IntWritable(1);

      public void map(BytesWritable k,
                      BytesWritable v,
                      OutputCollector<Text, Text> collector, 
                      Reporter reporter) 
        throws IOException 
      {

        try {
          // status
          //reporter.setStatus("processing " + item.getArcFileName());
          reporter.getCounter("DocVector.counters", "Files Processed").increment(1);

          // when dealing with seq files, we need to ignore the first byte. why?
          byte[] data = Arrays.copyOfRange(v.getBytes(), 4, v.getLength()); // copy only valid bytes
          byte[] url  = Arrays.copyOfRange(k.getBytes(), 4, k.getLength());

          Tika detector = new Tika();
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
             reporter.getCounter("DocVector.counters", "Valid Files Processed").increment(1);

             // split on <p> tags

             /* match */
             ArrayList<Integer> indexes = KMPMatch.allIndexOf(data, "<P ".getBytes());
             indexes.addAll(KMPMatch.allIndexOf(data,  "<P>".getBytes()));
             indexes.addAll(KMPMatch.allIndexOf(data, "<P\t".getBytes()));
             indexes.addAll(KMPMatch.allIndexOf(data,  "<P/".getBytes()));
             indexes.addAll(KMPMatch.allIndexOf(data,  "<p ".getBytes()));
             indexes.addAll(KMPMatch.allIndexOf(data,  "<p>".getBytes()));
             indexes.addAll(KMPMatch.allIndexOf(data, "<p\t".getBytes()));
             indexes.addAll(KMPMatch.allIndexOf(data,  "<p/".getBytes()));
  
             Collections.sort(indexes);

             // so we always start from the beginning of file
             if(!indexes.contains(new Integer(0))) {
               indexes.add(0, new Integer(0)); // first element
             }

             // slightly inaccurate because we consider from the start of the document
             reporter.getCounter("DocVector.counters", "Paragraph Chunks").increment(indexes.size());
  
             byte[] subArray; // temp array for stuff
             int data_length = data.length; // for byte arrays
             StringBuilder value = new StringBuilder(); // build our result
  
             // get array slices
             for(int i=0; i<indexes.size(); i++) {
               int b = indexes.get(i);
               int end;
               if(i != indexes.size()-1) {
                 end = indexes.get(i+1);
                 //System.out.println("Index: " + indexes.get(i) + "," + indexes.get(i+1));
               }
               else {
                 end = data_length; //XXX can optimize here...put data_length into indexes
                 //System.out.println("Index: " + indexes.get(i) + "," + x_len);
               }
  
               //System.out.println(x.substring(b,e));
               try {
                 //System.out.println("Index: " + b + ":" + end);
                 subArray = Arrays.copyOfRange(data, b, end);

                 // 2014-04-23: check for length >100 
                 if(subArray.length > 100) {
                   // hash contents
                   String hash = new String("");
                   try {
                     hash = Utilities.SHA256sum(subArray);
                     value.append(hash);
                     value.append(":");
                   } 
                   catch (NoSuchAlgorithmException e) {
                     e.printStackTrace();
                     reporter.getCounter("DocVector.exception", e.getClass().getSimpleName()).increment(1);
                   }
                 } //end if length > 100
               } // end try 
               catch (Exception e) {
                 e.printStackTrace();
                 reporter.getCounter("DocVector.exception", e.getClass().getSimpleName()).increment(1);
               }
             } // end for

              // collect output [URL, hash:hash:hash:]
              collector.collect(new Text(new String(url)), new Text(value.toString()));
          } //end if (valid datatypes)
          else {
            // do nothing
            reporter.getCounter("DocVector.counters", "Skipped Files").increment(1);
          }

          /////////////////////////////////////////////////////////////////////////////////////////////////////

          // debug:
          // collector.collect(new Text(key), new Text("1"));

        } // end try (process SEQ)
        catch(Exception e) {
          e.printStackTrace();
          reporter.getCounter("DocVector.exception", e.getClass().getSimpleName()).increment(1);
        } // end catch

      } // end public void map
    } // end public static class Map

    public static void main(String[] args) throws Exception 
    {
      // use this to automatically deal with -libjars option:
      // http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/util/ToolRunner.html
      ToolRunner.run(new DocVector(), args);
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
      conf.setOutputValueClass(Text.class);
      conf.setOutputFormat(TextOutputFormat.class);

      // Compress Map output
      conf.set("mapred.output.compress","true");
      conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
      conf.set("mapred.compress.map.output","true");
      conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
      //conf.set("mapred.job.priority", JobPriority.LOW.toString());

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      // run job
      JobClient.runJob(conf);

      return 0;
    }
}

//calvin@landercpu14:~$ hadoop fs -rmr /user/calvin/output-wp-hashfile-03Deleted hdfs://hcpu001.ant.isi.edu:6789/user/calvin/output-wp-hashfile-03
//calvin@landercpu14:~$ hadoop jar java-isi/build/isi-0.1.jar isi.wikipedia.HashFile /user/calvin/wikipedia-en-html-test.seq /user/calvin/output-wp-hashfile-03
