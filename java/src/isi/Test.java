package isi;

import java.io.*;
import java.io.IOException;
import java.io.FileInputStream;

import java.security.*;
import java.security.MessageDigest;

import java.util.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

import isi.util.KMPMatch;
import isi.util.Utilities;

public class Test 
{
    public void test_prune() {
        /* test heuristic */
        java.util.Map<String, Integer> occurrences = new HashMap<String, Integer>();
        
        String key_s = "geocities/YAHOOIDS/P/i/Pipeline/4966/islam/islam/www.usc.edu/dept/MSA/fundamentals/prophet/www.usc.edu/dept/MSA/fundamentals/prophet/www.usc.edu/dept/MSA/fundamentals/prophet/www.usc.edu/dept/MSA/fundamentals/prophet/islam/islam";

        for ( String word : key_s.split("/") ) {
           Integer oldCount = occurrences.get(word);
           if ( oldCount == null ) {
              oldCount = 0;
           }
           occurrences.put(word, oldCount + 1);
        }

        int max = Collections.max(occurrences.values());
        int prune = 0;
        //System.out.println(max);

        if(max >= 5) { prune = 1; } // prune if occurences >= 5

        Collection<Integer> c = occurrences.values();
        Iterator<Integer> iter = c.iterator();

        int c4 = 0;
        int c3 = 0;
        int c2 = 0;
        while(iter.hasNext()) {
          int val = (int)iter.next();
          if(val >= 5) { prune = 1; }
          if(val >= 4) { c4 = c4 + 1; }
          if(val >= 3) { c3 = c3 + 1; }
          if(val >= 2) { c2 = c2 + 1; }
        }

        //check
        if( c4 >= 1 || c3 >= 2 || c2 >= 2) { prune = 1; } 

        System.out.println("prune? " + prune + " " + c4 + " " + c3 + " " + c2);


    }

    public void test_regex() {
        //dump("<p>hi<p>hi</p><p>hi".split("(?=<[Pp][^rR])"));

        //FileInputStream fis = new FileInputStream("clown.html");
        //byte[] b = new byte[(int) fis.length()];  
        //fis.read(b);  
        
        /*

        dump("<p>hello</p><p>hi</p>".split("(?=<p>)"));
        dump("<p>hello</p><p>hi</p>".split("<p>"));
        dump("<p><p><p><p>hello</p><p>hi</p>".split("<p>"));
        dump("<p><p><p><p>hello</p><p>hi</p>".split("(?=<p>)"));
        dump("1,234,567,890".split(","));*/
        // "[1][234][567][890]"
/*
        dump("1,234,567,890".split("(?=,)"));   
        // "[1][,234][,567][,890]"
        dump("1,234,567,890".split("(?<=,)"));  
        // "[1,][234,][567,][890]"
        dump("1,234,567,890".split("(?<=,)|(?=,)"));
        // "[1][,][234][,][567][,][890]"

        dump(":a:bb::c:".split("(?=:)|(?<=:)"));
        // "[][:][a][:][bb][:][:][c][:]"
        dump(":a:bb::c:".split("(?=(?!^):)|(?<=:)"));
        // "[:][a][:][bb][:][:][c][:]"
        dump(":::a::::b  b::c:".split("(?=(?!^):)(?<!:)|(?!:)(?<=:)"));
        // "[:::][a][::::][b  b][::][c][:]"
        dump("a,bb:::c  d..e".split("(?!^)\\b"));
        // "[a][,][bb][:::][c][  ][d][..][e]"

        dump("ArrayIndexOutOfBoundsException".split("(?<=[a-z])(?=[A-Z])"));
        // "[Array][Index][Out][Of][Bounds][Exception]"
        dump("1234567890".split("(?<=\\G.{4})"));   
        // "[1234][5678][90]"

        // Split at the end of each run of letter
        dump("Boooyaaaah! Yippieeee!!".split("(?<=(?=(.)\\1(?!\\1))..)"));
        // "[Booo][yaaaa][h! Yipp][ieeee][!!]"*/

    }

    public static void test_bytearray() {
      System.out.println("*** testing byte array ***");
      // http://docs.oracle.com/javase/6/docs/technotes/guides/intl/encoding.doc.html

      try {
          String s = "<p>";
          byte[] s_utf8 = s.getBytes("UTF8");
          //System.out.println(s_utf8.toString());
          Utilities.printBytes(s_utf8, s);

          s_utf8 = s.getBytes("ISO8859_1");
          //System.out.println(s_utf8.toString());
          Utilities.printBytes(s_utf8, s);

          s_utf8 = s.getBytes("UTF-16");
          //System.out.println(s_utf8.toString());
          Utilities.printBytes(s_utf8, s);

          s_utf8 = s.getBytes("ASCII");
          //System.out.println(s_utf8.toString());
          Utilities.printBytes(s_utf8, s);

          s = "<P>";
          s_utf8 = s.getBytes("UTF8");
          //System.out.println(s_utf8.toString());
          Utilities.printBytes(s_utf8, s);

          s = "<P\t";
          s_utf8 = s.getBytes("UTF8");
          Utilities.printBytes(s_utf8, s);
          s = "<P ";
          s_utf8 = s.getBytes("UTF8");
          Utilities.printBytes(s_utf8, s);

          /////////////
          String x   = "<P>hello</p><p>hi</p><P>hello again<p><P><P><P>asdfasdfasdf";
          byte[] x_b = x.getBytes("UTF8");
          
          String w   = "<P";
          byte[] w_b = w.getBytes("UTF8");

          int lol = KMPMatch.indexOf(x_b, w_b);
          System.out.println(lol);

          ArrayList<Integer> indexes = KMPMatch.allIndexOf(x_b, "<P".getBytes("UTF8"));
          ArrayList<Integer> indexes2 = KMPMatch.allIndexOf(x_b, "<p".getBytes("UTF8"));

          indexes.addAll(indexes2);
          Collections.sort(indexes);

          int x_len = x.length();
          for(int i=0; i<x_len; i++) {
            if(i%10==0 && i != 0) {
              System.out.print(i/10);
            }
            else { System.out.print(" "); }
          }
          System.out.print("\n");
        
          for(int i=0; i<x_len; i++) {
            System.out.print(i%10);
          }
          System.out.print("\n");

          System.out.println(x);

          // print out substrings
          for(int i=0; i<indexes.size(); i++) {
            int b = indexes.get(i);
            int e;
            if(i != indexes.size()-1) {
              e = indexes.get(i+1);
              System.out.println("Index: " + indexes.get(i) + "," + indexes.get(i+1));
            }
            else {
              e = x_len;
              System.out.println("Index: " + indexes.get(i) + "," + x_len);
            }

            System.out.println(x.substring(b,e));
          }

      }
      catch (UnsupportedEncodingException e) {
          e.printStackTrace();
      }
    }

    public static void test_stringcontainment() {
      Set<String> valid_filetypes  = new HashSet<String>();
      valid_filetypes.addAll(Arrays.asList(
         "application/xml",
         "application/rss+xml",
         "text/plain",
         "text/html",
         "application/xhtml+xml"
      ));

      System.out.println("invalid: " + valid_filetypes.contains("hi"));
      System.out.println("valid: " + valid_filetypes.contains("text/html"));
      System.out.println("valid: " + valid_filetypes.contains("application/xml"));
      System.out.println("valid: " + valid_filetypes.contains("application/rss+xml"));
      System.out.println("valid: " + valid_filetypes.contains("text/plain"));
      System.out.println("valid: " + valid_filetypes.contains("text/html"));
      System.out.println("valid: " + valid_filetypes.contains("application/xhtml+xml"));
    }

    public static void main(String[] args) 
    {
      //test_bytearray();
      test_stringcontainment();
    }
}
