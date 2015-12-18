package com.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class MapperFor100 extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {
	
	private TreeMap<IntWritable, Text> map = new TreeMap<IntWritable, Text>();
	public void map(Object key, Text value, OutputCollector<IntWritable, Text> collector, Reporter arg3) throws IOException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        {
            int number = 999;
            String word = "empty";

            if (stringTokenizer.hasMoreTokens()) {
                String str0 = stringTokenizer.nextToken();
                word = str0.trim();
            }

            if (stringTokenizer.hasMoreElements()) {
                String str1 = stringTokenizer.nextToken();
                number = Integer.parseInt(str1.trim());
            }
            collector.collect(new IntWritable(number), new Text(word));
            
        }

    }

 }

