package com.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount2 {
           
	    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	       private final static IntWritable one = new IntWritable(1);
	       private Text word = new Text();
	           
	       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	           String line = value.toString();
	           StringTokenizer tokenizer = new StringTokenizer(line);
	           while (tokenizer.hasMoreTokens()) {
	               word.set(tokenizer.nextToken());
	               context.write(word, one);
	           }
	       }
	    } 
	           
	    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	   
	       public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	         throws IOException, InterruptedException {
	           int sum = 0;
	           for (IntWritable val : values) {
	               sum += val.get();
	           }
	           context.write(key, new IntWritable(sum));
	       }
	    }
	    
	    public static class Map1 extends Mapper<Object, Text, IntWritable, Text>{
	    	TreeMap<IntWritable, Text> map = new TreeMap<IntWritable, Text>();
	    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
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
	                map.put(new IntWritable(number), new Text(word));
	                if(map.size() > 100){
	                	map.remove(map.firstKey());
	                }
	            }
	            
	    	}
	    	@Override
			protected void cleanup(Context context) throws IOException,
					InterruptedException {
	    		Set set = map.entrySet();
	            Iterator i = set.iterator();
	            while(i.hasNext()){
	            	Map.Entry<IntWritable, Text> me = (Map.Entry)i.next();
	            	context.write(me.getKey(), new Text(me.getValue()));
	            }
			}
	    }
	    
	    public static class Reducer2 extends Reducer<IntWritable, Text, IntWritable, Text>{
	    	TreeMap<IntWritable, Text> map = new TreeMap<IntWritable, Text>();
	    	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	            for(Text value : values){
	            	map.put(key, value);
	            }
	            if(map.size() > 100){
	            	map.remove(map.firstKey());
	            }
	            Set set = map.entrySet();
	            Iterator i = set.iterator();
	            while(i.hasNext()){
	            	Map.Entry<IntWritable, Text> me = (Map.Entry)i.next();
	            	context.write(me.getKey(), new Text(me.getValue()));
	            }
	        }
	    }
	    
	    public static class MapperLength extends Mapper<LongWritable, Text, Text, IntWritable> {
		       private final static IntWritable one = new IntWritable(1);
		       private Text word = new Text();
		       TreeMap<IntWritable, Text> map = new TreeMap<IntWritable, Text>();
		       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    	   Configuration conf = context.getConfiguration();
		    	   int wordLength = Integer.parseInt(conf.get("wordlength"));
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
		                if(word.length() == wordLength){
		               //context.write(word, one);
		            	   map.put(new IntWritable(number), new Text(word));
			                
		        	   }
		               if(map.size() > 100){
		                	map.remove(map.firstKey());
		                }
		            }
		       }
		       
		       
		       
		       @Override
				protected void cleanup(Context context) throws IOException,
						InterruptedException {
		    		Set set = map.entrySet();
		            Iterator i = set.iterator();
		            while(i.hasNext()){
		            	Map.Entry<IntWritable, Text> me = (Map.Entry)i.next();
		            	context.write(new Text(me.getValue()), me.getKey());
		            }
				}
		    } 
		           
		    public static class ReduceLength extends Reducer<Text, IntWritable, Text, IntWritable> {
		       TreeMap<IntWritable, Text> map = new TreeMap<IntWritable, Text>();
		       public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		         throws IOException, InterruptedException {
		    	   for(IntWritable value : values){
		            	map.put(value, key);
		            }
		            if(map.size() > 100){
		            	map.remove(map.firstKey());
		            }
		            Set set = map.entrySet();
		            Iterator i = set.iterator();
		            while(i.hasNext()){
		            	Map.Entry<IntWritable, Text> me = (Map.Entry)i.next();
		            	context.write(new Text(me.getValue()), me.getKey());
		            }
		       }
		    }
		    
		    public static class MapperPref extends Mapper<LongWritable, Text, Text, IntWritable> {
			       TreeMap<IntWritable, Text> map = new TreeMap<IntWritable, Text>();
			       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			    	   Configuration conf = context.getConfiguration();
			    	   String wordPref = conf.get("wordPref");
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
			                if(word.startsWith(wordPref)){
			                	map.put(new IntWritable(number), new Text(word));
			                }
			                
			        	   if(map.size() > 100){
			                	map.remove(map.firstKey());
			                }		
			           	}
			           
			       }
			       @Override
					protected void cleanup(Context context) throws IOException,
							InterruptedException {
			    		Set set = map.entrySet();
			            Iterator i = set.iterator();
			            while(i.hasNext()){
			            	Map.Entry<IntWritable, Text> me = (Map.Entry)i.next();
			            	context.write(new Text(me.getValue()), me.getKey());
			            }
					}
			    } 
			           
			    public static class ReducePref extends Reducer<Text, IntWritable, Text, IntWritable> {
			    	TreeMap<IntWritable, Text> map = new TreeMap<IntWritable, Text>();
			       public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			         throws IOException, InterruptedException {
			    	   for(IntWritable value : values){
			            	map.put(value, key);
			            }
			            if(map.size() > 100){
			            	map.remove(map.firstKey());
			            }
			            Set set = map.entrySet();
			            Iterator i = set.iterator();
			            while(i.hasNext()){
			            	Map.Entry<IntWritable, Text> me = (Map.Entry)i.next();
			            	context.write(new Text(me.getValue()), me.getKey());
			            }
			       }
			    }
	    
			    public static class DescendingKeyComparator extends WritableComparator {
			        protected DescendingKeyComparator() {
			            super(IntWritable.class, true);
			        }

			        @SuppressWarnings("rawtypes")
			        @Override
			        public int compare(WritableComparable w1, WritableComparable w2) {
			        	IntWritable key1 = (IntWritable) w1;
			        	IntWritable key2 = (IntWritable) w2;          
			            return -1 * key1.compareTo(key2);
			        }
			    }
			    
	    public static void main(String[] args) throws Exception {
	       
	       Configuration conf = new Configuration();
	       Configuration conf2 = new Configuration();
	       Configuration conf3 = new Configuration();
	       Configuration conf4 = new Configuration();
	       Job job = new Job(conf, "wordcount");
	       
	       job.setOutputKeyClass(Text.class);
	       job.setOutputValueClass(IntWritable.class);
	           
	       job.setMapperClass(Mapper1.class);
	       job.setReducerClass(Reduce.class);
	           
	       job.setInputFormatClass(TextInputFormat.class);
	       job.setOutputFormatClass(TextOutputFormat.class);
	           
	       FileInputFormat.addInputPath(job, new Path(args[0]));
	       FileOutputFormat.setOutputPath(job, new Path(args[1]));
	           
	       Job job2 = new Job(conf2, "worcount3");
	             
	       job2.setSortComparatorClass(DescendingKeyComparator.class);
	       job2.setOutputKeyClass(IntWritable.class);
	       job2.setOutputValueClass(Text.class);
	       
	       job2.setMapperClass(Map1.class);
	       job2.setReducerClass(Reducer2.class);
	           
	       job2.setInputFormatClass(TextInputFormat.class);
	       job2.setOutputFormatClass(TextOutputFormat.class);
	       
	       FileInputFormat.addInputPath(job2, new Path("out/part-r-00000"));
	       FileOutputFormat.setOutputPath(job2, new Path("a_"+args[1]));
	       
	       String strLength = args[2];
	       conf3.set("wordlength", strLength);
	       Job job3 = new Job(conf3, "WordLength");
	       
	       job3.setSortComparatorClass(DescendingKeyComparator.class);
	       
	       job3.setOutputKeyClass(Text.class);
	       job3.setOutputValueClass(IntWritable.class);
	       
	       job3.setMapperClass(MapperLength.class);
	       job3.setReducerClass(ReduceLength.class);
	       
	       job3.setInputFormatClass(TextInputFormat.class);
	       job3.setOutputFormatClass(TextOutputFormat.class);

	       FileInputFormat.addInputPath(job3, new Path("out/part-r-00000"));
	       FileOutputFormat.setOutputPath(job3, new Path("b_"+args[1]));
	       
	       String prefix = args[3];
	       conf4.set("wordPref", prefix);
	       Job job4 = new Job(conf4, "WordPrefi");
	       
	       job4.setSortComparatorClass(DescendingKeyComparator.class);
	       
	       job4.setOutputKeyClass(Text.class);
	       job4.setOutputValueClass(IntWritable.class);
	       
	       job4.setMapperClass(MapperPref.class);
	       job4.setReducerClass(ReducePref.class);
	       
	       job4.setInputFormatClass(TextInputFormat.class);
	       job4.setOutputFormatClass(TextOutputFormat.class);

	       FileInputFormat.addInputPath(job4, new Path("out/part-r-00000"));
	       FileOutputFormat.setOutputPath(job4, new Path("c_"+args[1]));
	       
	       job.submit();
	       if(job.waitForCompletion(true)){
	    	   job2.submit(); 
	    	   if(job2.waitForCompletion(true)){
	    		   job3.submit();
	    		   if(job3.waitForCompletion(true)){
	    			   job4.submit();
	    			   job4.waitForCompletion(true);
	    		   }
	    	   }
	       }
	    }
	           
}

