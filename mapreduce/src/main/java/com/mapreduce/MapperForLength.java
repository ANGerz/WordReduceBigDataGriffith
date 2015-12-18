package com.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapperForLength extends Mapper <LongWritable, Text, Text, IntWritable > {
    static String wordToSearch;
    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    
    public void map(Text key, Text value, Context context)
    throws IOException, InterruptedException {
        //Create a new configuration
        Configuration conf = context.getConfiguration();
        //retrieve the wordToSearch variable
        String wordLength = conf.get("wordLength");
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
        	if(tokenizer.nextToken().length() == Integer.parseInt(wordLength)){
        		word.set(tokenizer.nextToken());
        		context.write(word, ONE);
        	}
          
        }
    }
}
