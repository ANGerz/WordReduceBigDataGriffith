package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> values, 
			Context context) throws IOException, InterruptedException {
       
		int sum = 0;      
        for(IntWritable val : values) {
          sum += val.get();
        }
        
        //output.collect(key, new IntWritable(sum));
        context.write(key, new IntWritable(sum));
	}
}

