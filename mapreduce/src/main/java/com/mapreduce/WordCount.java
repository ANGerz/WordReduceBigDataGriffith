package com.mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@SuppressWarnings("deprecation")
public class WordCount {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
        
    	//JobConf conf = new JobConf(WordCount.class);
		Configuration conf = new Configuration();
        Job job1 = new Job(conf, "worcount");
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));        
        
//        conf.setJobName("wordCount");
//
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(IntWritable.class);
//
//        conf.setMapperClass(Map.class);
//        conf.setCombinerClass(Reduce.class);
//        conf.setReducerClass(Reduce.class);
//        
//        conf.setInputFormat(TextInputFormat.class);
//        conf.setOutputFormat(TextOutputFormat.class);

//        FileInputFormat.setInputPaths(conf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
       
        
       
        /*JobConf conf2 = new JobConf(WordCount.class);        
        conf2.setJobName("WordCount1");

        conf2.setOutputKeyClass(IntWritable.class);
        conf2.setOutputValueClass(Text.class);

        conf2.setMapperClass(MapperFor100.class);
        conf2.setReducerClass(ReduceFor100.class);
        conf2.setCombinerClass(ReduceFor100.class);

        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        */
        //Job job1 = new Job(conf);
        //Job job2 = new Job(conf2);
        
        job1.waitForCompletion(true);
        
//        if (job1.waitForCompletion(true)) {
//            job2.submit();
//            job2.waitForCompletion(true);
//        }
		
        
    } 
}