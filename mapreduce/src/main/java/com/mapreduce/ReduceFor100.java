package com.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class ReduceFor100 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable,Text> {
	String column_delimiter;
		int column_to_rank;
		int rank_limit,rank_key;
		Boolean sort_ascending;
		public void setup(Context context) 	{
			Configuration conf = context.getConfiguration();
			column_delimiter = conf.get("column.delimiter");
			column_to_rank = conf.getInt("column.to.rank", Integer.MIN_VALUE) - 1;
			rank_limit = conf.getInt("rank.limit", Integer.MIN_VALUE);
			sort_ascending = conf.getBoolean("sort.ascending", true);
			rank_key=conf.getInt("rank.keys", Integer.MIN_VALUE)-1;			
		}	
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> arg2, Reporter arg3) throws IOException {
		TreeMap<IntWritable, Text> map = new TreeMap<IntWritable, Text>();
		while ((values.hasNext())) {           
        	map.put(key, values.next());
            if(map.size() > 100){
            	if (sort_ascending) 
				{
					map.remove(map.lastKey());
				}else {
            	map.remove(map.firstKey());
				}
            }            
		}
        
        Set set = map.entrySet();
        Iterator i = set.iterator();
        while(i.hasNext()){
        	Map.Entry<IntWritable, Text> me = (Map.Entry)i.next();
        	arg2.collect(me.getKey(), new Text(me.getValue()));
        }
    }
}
