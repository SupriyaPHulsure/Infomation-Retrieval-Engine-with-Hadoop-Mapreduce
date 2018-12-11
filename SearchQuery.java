

package miniGoogle;


import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class SearchQuery {
	

	 public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	  { 
	   // private final static IntWritable one = new IntWritable(1);
		//private Text term = new Text();
		 String ip1;
		 
		 public void configure(JobConf job) {
		    ip1 = job.get("stringkey").toLowerCase();
		}
		 
	    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException 
	    {
	    	String word, filename;
	    	int frequency;
	    	String delims = "[\t{}]+";
	        String line = value.toString();
	        String[] tokens = line.split(delims);
	        
	        word = tokens[0];
	      
			String [] filenameswithfrequency = tokens[1].split("[=,]");
			
			for (int j = 0; j+1 < filenameswithfrequency.length; j= j+2) {
				 filename = filenameswithfrequency[j];
				 frequency = Integer.parseInt(filenameswithfrequency[j+1]);
				String a  = filename + ":" + frequency;
				 output.collect(new Text(word),new Text(a));
			}
				
		}
	 }
	 
	 
	
	 
	public static class Reduce1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
	 {
		
		String ip1, ip2;
		 String[] tokens;
		 
		 public void configure(JobConf job) {
		    ip1 = job.get("stringkey").toLowerCase();
		    tokens = ip1.split(" ");
		}
		 
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			HashMap<String, Integer> hmap1 = new HashMap<String, Integer>();			
			String hmapstring;
			
			while(values.hasNext()) {
				hmapstring = values.next().toString();
				String[] keyValue = hmapstring.split(":");
				hmap1.put(keyValue[0], Integer.valueOf(keyValue[1]));
				
			}
			
			Map<String, Integer> map = sortByValues(hmap1); 
		      
		    for(int j = 4; j < tokens.length; j++) {
				//System.out.println("keyword input -"+j+tokens[j]);
				if(new String(key.toString()).equals(tokens[j]))
					output.collect(key, new Text(map.toString()));
			}
		}
			
	 }
	
	private static HashMap sortByValues(HashMap map) { 
	       LinkedList list = new LinkedList(map.entrySet());
	       // Defined Custom Comparator here
	       Collections.sort(list, new Comparator() {
	            public int compare(Object o1, Object o2) {
	               return -1*((Comparable) ((Map.Entry) (o1)).getValue())
	                  .compareTo(((Map.Entry) (o2)).getValue());
	            }
	       });

	       // Here I am copying the sorted list in HashMap
	       // using LinkedHashMap to preserve the insertion order
	       HashMap sortedHashMap = new LinkedHashMap();
	       for (Iterator it = list.iterator(); it.hasNext();) {
	              Map.Entry entry = (Map.Entry) it.next();
	              sortedHashMap.put(entry.getKey(), entry.getValue());
	       } 
	     return sortedHashMap;
}
	
	
	public static void main(String[] args) throws Exception 
{
	 
	
	JobConf conf1 = new JobConf(SearchQuery.class);
	conf1.setJobName("SearchKeywords");
	
	if(args.length > 2)
   {
		conf1.set("stringkey", String.join( " ", args ));
   } 
	
	conf1.setOutputValueClass(Text.class);
	conf1.setOutputKeyClass(Text.class);
	
	conf1.setMapperClass(Map1.class);
	conf1.setReducerClass(Reduce1.class);
	conf1.setNumMapTasks(Integer.parseInt(args[2]));
	conf1.setNumReduceTasks(Integer.parseInt(args[3]));
	//conf1.setNumReduceTasks(0);

		
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		
		Path outputPath1 = new Path(args[1]);
		
		FileOutputFormat.setOutputPath(conf1,outputPath1);
		
		
		 FileSystem fs1 = outputPath1.getFileSystem(conf1);
		 if (fs1.exists(outputPath1))
            fs1.delete(outputPath1, true);
		
		
		JobClient.runJob(conf1);
		

 }
}





