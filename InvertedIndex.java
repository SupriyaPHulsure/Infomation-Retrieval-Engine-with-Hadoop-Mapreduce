package miniGoogle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.ByteArrayManager.Conf;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.htrace.core.StandardOutSpanReceiver;

public class InvertedIndex {
	
	 public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
  { 
   // private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
        
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
    {
    	Path filepath = ((FileSplit) reporter.getInputSplit()).getPath();
    	String filename = filepath.getName();
        String line = value.toString().toLowerCase();
        String[] words = line.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
        //StringTokenizer tokenizer = new StringTokenizer(line);
        for (int j = 0; j < words.length; j= j+1) {
        //while (tokenizer.hasMoreTokens()) 
        //{ 
            word.set(words[j]);
            output.collect(word, new Text(filename));
        }
    }
 }
	  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
 {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			HashMap m=new HashMap();
			int count=0;
			while (values.hasNext()) 
			{
				String str= values.next().toString();
				/*Check if file name is present in the HashMap ,if File name is not present then add the Filename to the HashMap and increment the counter by one , This condition will be satisfied on first occurrence of that word*/
				if(m!=null &&m.get(str)!=null){
					count=(int)m.get(str);
					m.put(str, ++count);
				}else{
				/*Else part will execute if file name is already added then just increase the count for that file name which is stored as key in the hash map*/
					m.put(str, 1);
				}
			}
 			output.collect(key, new Text(m.toString()));
 		}
 }


			public static void main(String[] args) throws Exception 
		{
				long start = new Date().getTime(); 
				//boolean status = job.waitForCompletion(true); 
				
			JobConf conf = new JobConf(InvertedIndex.class);
			conf.setJobName("InvertedIndex");
			
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			
			conf.setMapperClass(Map.class);
			conf.setNumMapTasks(Integer.parseInt(args[2]));
			conf.setNumReduceTasks(Integer.parseInt(args[3]));
			conf.setReducerClass(Reduce.class);
			
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			
			Path outputPath = new Path(args[1]);
			
			FileOutputFormat.setOutputPath(conf,outputPath);
			
			
			 FileSystem fs = outputPath.getFileSystem(conf);
			 if (fs.exists(outputPath))
	            fs.delete(outputPath, true);
			
			
			JobClient.runJob(conf);
			
			long end = new Date().getTime(); 
			//System.out.println("Job took "+(end-start) + "milliseconds");
			
	 }




		  
}