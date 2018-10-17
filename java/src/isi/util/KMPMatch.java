package isi.util;

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

/**
 * Knuth-Morris-Pratt Algorithm for Pattern Matching
 */
public class KMPMatch {
    /**
     * Finds the first occurrence of the pattern in the text.
     */
    public static int indexOf(byte[] data, byte[] pattern) {
        int[] failure = computeFailure(pattern);

        int j = 0;
        if (data.length == 0) return -1;

        for (int i = 0; i < data.length; i++) {
            while (j > 0 && pattern[j] != data[i]) {
                j = failure[j - 1];
            }
            if (pattern[j] == data[i]) { j++; }
            if (j == pattern.length) {
                return i - pattern.length + 1;
            }
        }
        return -1;
    }

    public static int indexOf(byte[] data, byte[] pattern, int offset) {
        int[] failure = computeFailure(pattern);

        int j = 0;
        if (data.length == 0) return -1;

        int max = data.length - offset;
        if (max <= 0) return -1;

        for (int i = offset; i < data.length; i++) {
            while (j > 0 && pattern[j] != data[i]) {
                j = failure[j - 1];
            }
            if (pattern[j] == data[i]) { j++; }
            if (j == pattern.length) {
                return i - pattern.length + 1;
            }
        }
        return -1;
    }

    /**
     * Computes the failure function using a boot-strapping process,
     * where the pattern is matched against itself.
     */
    private static int[] computeFailure(byte[] pattern) {
        int[] failure = new int[pattern.length];

        int j = 0;
        for (int i = 1; i < pattern.length; i++) {
            while (j > 0 && pattern[j] != pattern[i]) {
                j = failure[j - 1];
            }
            if (pattern[j] == pattern[i]) {
                j++;
            }
            failure[i] = j;
        }

        return failure;
    }

    public static ArrayList<Integer> allIndexOf(byte[] data, byte[] pattern) {
      ArrayList<Integer> indexes = new ArrayList<Integer>();

      int index = indexOf(data, pattern);
      //System.out.println("First Index: " + index);
      if(index != -1) {
        indexes.add(index);
      }
      while(index >= 0) {
        //System.out.println("Index: "+index);
        index = indexOf(data, pattern, index+pattern.length);
        if(index != -1) {
          indexes.add(index);
        }
      }
      return indexes;
 
    }
  
    /*public static ArrayList<Integer> allIndexOf(byte[] data, byte[] pattern) {
      ArrayList<Integer> indexes = new ArrayList<Integer>();

      int index = indexOf(data, pattern);
      indexes.add(index);
      while(index >= 0) {
        //System.out.println("Index: "+index);
        index = indexOf(data, pattern, index+pattern.length);
        if(!(index <= -1))
          indexes.add(index);
      }
      return indexes;
    }*/
}
