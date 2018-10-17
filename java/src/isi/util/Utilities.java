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

public class Utilities
{
    static public void dump(String[] arr) {
        for (String s : arr) {
            //if(s.compareTo("") == 0) { System.out.println("lol"); }
            System.out.format("[%s]", s);
        }
        System.out.println();
    }


    static public String byteToHex(byte b) {
       // Returns hex String representation of byte b
       char hexDigit[] = {
          '0', '1', '2', '3', '4', '5', '6', '7',
          '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
       };
       char[] array = { hexDigit[(b >> 4) & 0x0f], hexDigit[b & 0x0f] };
       return new String(array);
    }

    static public String charToHex(char c) {
       // Returns hex String representation of char c
       byte hi = (byte) (c >>> 8);
       byte lo = (byte) (c & 0xff);
       return byteToHex(hi) + byteToHex(lo);
    }

    static public void printBytes(byte[] array, String name) {
      for (int k = 0; k < array.length; k++) {
         long unsignedInt = array[k];
         System.out.println(name + "[" + k + "] = " + "0x" +
            byteToHex(array[k]) + " 0d" + unsignedInt);
      }
    }

    /////////////////////////////////////////////////////////////

    public static byte[] hexStringToByteArray(String s) 
    {
      int len = s.length();
      byte[] data = new byte[len / 2];
      for (int i = 0; i < len; i += 2) {
          data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                               + Character.digit(s.charAt(i+1), 16));
      }
      return data;
    }

    public static String byteArray2Hex(final byte[] hash) 
    {
      Formatter formatter = new Formatter();
      for (byte b : hash) {
          formatter.format("%02x", b);
      }
      return formatter.toString();
    }

    public static String SHAsum(byte[] convertme) 
      throws NoSuchAlgorithmException
    {
      MessageDigest md = MessageDigest.getInstance("SHA-1"); 
      return byteArray2Hex(md.digest(convertme));
    }

    public static String SHA256sum(byte[] convertme) 
      throws NoSuchAlgorithmException
    {
      MessageDigest md = MessageDigest.getInstance("SHA-256"); 
      return byteArray2Hex(md.digest(convertme));
    }
}
