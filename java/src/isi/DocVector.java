package isi;

import java.io.IOException;

import java.util.*;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
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
//import org.apache.tika.detect.MagicDetector;
//import org.apache.tika.mime.MediaType;
import org.apache.tika.Tika;
//import java.io.ByteArrayInputStream;

import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.HdfsARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;

import isi.util.KMPMatch;
import isi.util.Utilities;

public class DocVector extends Configured implements Tool 
{
    public static class Map extends MapReduceBase 
      implements Mapper<Text, ArcFileItem, Text, Text> 
    {

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

              reporter.getCounter("DocVector.counters", "Processed Files").increment(1);

              // split on <p> tags

              /* match */
              ArrayList<Integer> indexes = KMPMatch.allIndexOf(data, "<P ".getBytes("UTF8"));
              indexes.addAll(KMPMatch.allIndexOf(data,  "<P>".getBytes("UTF8")));
              indexes.addAll(KMPMatch.allIndexOf(data, "<P\t".getBytes("UTF8")));
              indexes.addAll(KMPMatch.allIndexOf(data,  "<P/".getBytes("UTF8")));
              indexes.addAll(KMPMatch.allIndexOf(data,  "<p ".getBytes("UTF8")));
              indexes.addAll(KMPMatch.allIndexOf(data,  "<p>".getBytes("UTF8")));
              indexes.addAll(KMPMatch.allIndexOf(data, "<p\t".getBytes("UTF8")));
              indexes.addAll(KMPMatch.allIndexOf(data,  "<p/".getBytes("UTF8")));
  
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
  
                  // hash contents
                  String hash = new String("");
                  try {
                    hash = Utilities.SHAsum(subArray);
                    value.append(hash);
                    value.append(":");
                  } 
                  catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                    reporter.getCounter("DocVector.exception", e.getClass().getSimpleName()).increment(1);
                  }
                
                } // end try 
                catch (Exception e) {
                  e.printStackTrace();
                  reporter.getCounter("DocVector.exception", e.getClass().getSimpleName()).increment(1);
                }
              } // end for

              // collect output [URL, hash:hash:hash:]
              collector.collect(url, new Text(value.toString()));
            } //end if in valid datatypes
            else {
              // do nothing
              reporter.getCounter("DocVector.counters", "Skipped Files").increment(1);
            }
          } 
          catch (IOException e) {
            e.printStackTrace();
            reporter.getCounter("DocVector.exception", e.getClass().getSimpleName()).increment(1);
          } // end try (media detection)

        } // end try (process ARC)
        catch(Exception e) {
          e.printStackTrace();
          reporter.getCounter("DocVector.exception", e.getClass().getSimpleName()).increment(1);
        } // end catch
      } // end map()
    } // end Map class     

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

      Path input_path = new Path(args[0]);
      String input_path_s = input_path.getName();

      // check file extension
      /*
      int mid = input_path_s.lastIndexOf("."); // find position of /abcd/efgh/file.html
      String ext = input_path_s.substring(mid+1);

      if( ext.equals("gz") ) {
        conf.setNumReduceTasks(1);
      } else {
        conf.setMemoryForReduceTask(1400); // in MB
      }*/

      // set up ARCInputFormat options here
      conf.setInputFormat(ARCInputFormat.class);
      ARCInputFormat.setARCSourceClass(conf, HdfsARCSource.class);
      ARCInputFormat inputFormat = new ARCInputFormat();
      inputFormat.configure(conf);

      // Mapper Class
      conf.setMapperClass(Map.class);

      // Reducer Class
      //conf.setReducerClass(Reduce.class);
      conf.setNumReduceTasks(0);

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
} // end class
