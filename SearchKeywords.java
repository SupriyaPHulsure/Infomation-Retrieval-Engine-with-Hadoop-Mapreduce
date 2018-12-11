package miniGoogle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class SearchKeywords {
	

	 public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, KeywordFreqpair, Text> 
	  { 
	   // private final static IntWritable one = new IntWritable(1);
		//private Text term = new Text();
		 String ip1;
		 
		 public void configure(JobConf job) {
		    ip1 = job.get("stringkey").toLowerCase();
		}
		 
	    public void map(LongWritable key, Text value, OutputCollector<KeywordFreqpair, Text> output, Reporter reporter) throws IOException 
	    {
	    	String word, filename;
	    	int frequency;
	    	//InvertedIndex II = new InvertedIndex();
	    	KeywordFreqpair keyfreq = new KeywordFreqpair();
	    	String delims = "[\t{}]+";
	        String line = value.toString();
	        String[] tokens = line.split(delims);
	        
	        word = tokens[0];
	      
			String [] filenameswithfrequency = tokens[1].split("[=,]");
			
			
			for (int j = 0; j+1 < filenameswithfrequency.length; j= j+2) {
				 filename = filenameswithfrequency[j];
				 frequency = Integer.parseInt(filenameswithfrequency[j+1]);
				 keyfreq.setTerm(word);
				 keyfreq.setFrequency(frequency);
				 keyfreq.setFilename(filename);
				 System.out.println("MAp Output: "+ new Text(word) + " " +new Text(filename)+" "+ frequency);
				
				 output.collect(keyfreq, new Text(filename));
			}
				
		}
	 }
	 
	 
	 public static class KeywordFreqpair implements Writable, WritableComparable<KeywordFreqpair> {
	 
	      private Text term = new Text();                 // natural key
	      private IntWritable frequency = new IntWritable(); // secondary key
	      private Text filename = new Text();
	
	     
	     public int compareTo(KeywordFreqpair pair) {
	         int compareValue = this.term.compareTo(pair.getTerm());
	         if (compareValue == 0) {
	             compareValue = frequency.compareTo(pair.getFrequency());
	         }
	         //return compareValue;    // sort ascending
	         return -1*compareValue;   // sort descending
	     }


		public void setFilename(String filename2) {
			// TODO Auto-generated method stub
			filename = new Text(filename2);
			
		}


		public void setFrequency(int frequency2) {
			// TODO Auto-generated method stub
			frequency = new IntWritable(frequency2);
		}


		public void setTerm(String word) {
			// TODO Auto-generated method stub
			term = new Text(word);
		}


		private IntWritable getFrequency() {
			// TODO Auto-generated method stub
			return frequency;
		}


		private BinaryComparable getTerm() {
			// TODO Auto-generated method stub
			return term;
		}


		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			term.readFields(arg0);
			frequency.readFields(arg0);
			filename.readFields(arg0);
		}


		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			
			term.write(arg0);
			frequency.write(arg0);
			filename.write(arg0);
			
		}
	     
	 }
	 
	 public static class map1Partitioner implements Partitioner<KeywordFreqpair, Text> {
	 
		 JobConf conf;
	      @Override
	      public int getPartition(KeywordFreqpair pair,
	                              Text text,
	                             int numberOfPartitions) {
	         // make sure that partitions are non-negative
	         return Math.abs(pair.getTerm().hashCode() % numberOfPartitions);
	      }

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			 this.conf = arg0;
		}
	 }
	 
	 public static class KeywordFreqeGroupingComparator extends WritableComparator {
		 
		 	public KeywordFreqeGroupingComparator() {
		 		super(KeywordFreqpair.class, true);
		 	}
	
	     @Override
	     /**
	      * This comparator controls which keys are grouped
	      * together into a single call to the reduce() method
	      */
	     public int compare(WritableComparable wc1, WritableComparable wc2) {
	    	 KeywordFreqpair pair1 = (KeywordFreqpair) wc1;
	    	 KeywordFreqpair pair2 = (KeywordFreqpair) wc2;
	    	 
	    	
	    	 int compare = pair1.getTerm().compareTo(pair2.getTerm());
//	    	 if (compare == 0) {
//	    		 compare = pair1.getFrequency().compareTo(pair2.getFrequency());
//	         }
	         //return compareValue;    // sort ascending
	         //return -1*compareValue;

	    	 return compare;
	         
	     }
	 }
	
	 public static class CompositeKeyComparator extends WritableComparator {
		 
		 	public CompositeKeyComparator() {
		 		super(KeywordFreqpair.class, true);
		 	}
	
	     @Override
	     /**
	      * This comparator controls which keys are grouped
	      * together into a single call to the reduce() method
	      */
	     public int compare(WritableComparable wc1, WritableComparable wc2) {
	    	 KeywordFreqpair pair1 = (KeywordFreqpair) wc1;
	    	 KeywordFreqpair pair2 = (KeywordFreqpair) wc2;
	    	 
	    	
	    	 int compare = pair1.getTerm().compareTo(pair2.getTerm());
	    	 if (compare == 0) {
	    		 compare = pair1.getFrequency().compareTo(pair2.getFrequency());
	         }
	         //return compareValue;    // sort ascending
	         //return -1*compareValue;

	    	 return -1*compare;
	         
	     }
	 }
	 
	public static class Reduce1 extends MapReduceBase implements Reducer<KeywordFreqpair, Text, Text, Text> 
	 {
		
		String ip1, ip2;
		 String[] tokens;
		 
		 public void configure(JobConf job) {
		    ip1 = job.get("stringkey");
		    //ip2 = job.get("stringkey2");
		    tokens = ip1.split(" ");
		    //System.out.println("key word - "+ip1);
		}
		 
			public void reduce(KeywordFreqpair key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
			{
				StringBuilder strings = new StringBuilder();
				int sum = 0; 
				strings.append(" -> ");
				System.out.println("reducer input: "+ key.term + " " + key.filename+" "+ key.frequency);
				while(values.hasNext()) {
					//strings.append("(");
					strings.append(values.next());
					//strings.append(key.frequency);					
					//strings.append(")");
					strings.append(",  ");
				}
				//strings.append(sum);
				//strings.append(")");
				//strings.append(", ");
				
				
				System.out.println("tokens[] length- "+ tokens.length);
				for(int j = 4; j < tokens.length; j++) {
					//System.out.println("keyword input -"+j+tokens[j]);
					if(new String(key.term.toString()).equals(tokens[j]))
						output.collect(key.term, new Text(strings.toString()));
				}
				
	 		}
	 }

			public static void main(String[] args) throws Exception 
		{
//			JobConf conf = new JobConf(InvertedIndex.class);
//			conf.setJobName("InvertedIndex");
//			
//			conf.setOutputKeyClass(Text.class);
//			conf.setOutputValueClass(Text.class);
//			
//			conf.setMapperClass(Map.class);
//			conf.setReducerClass(Reduce.class);
//			
//			conf.setInputFormat(TextInputFormat.class);
//			conf.setOutputFormat(TextOutputFormat.class);
//			
//			FileInputFormat.setInputPaths(conf, new Path(args[0]));
//			
//			Path outputPath = new Path(args[1]);
//			
//			FileOutputFormat.setOutputPath(conf,outputPath);
//			
//			
//			 FileSystem fs = outputPath.getFileSystem(conf);
//			 if (fs.exists(outputPath))
//	            fs.delete(outputPath, true);
//			
//			
//			JobClient.runJob(conf);
//			
			 
			
			JobConf conf1 = new JobConf(SearchKeywords.class);
			conf1.setJobName("SearchKeywords");
			
			if(args.length > 2)
           {
				conf1.set("stringkey", String.join( " ", args ));
           } 
			
			conf1.setOutputKeyClass(KeywordFreqpair.class);
			conf1.setOutputValueClass(Text.class);
			
			conf1.setMapperClass(Map1.class);
			conf1.setReducerClass(Reduce1.class);
			conf1.setNumMapTasks(Integer.parseInt(args[2]));
			conf1.setNumReduceTasks(Integer.parseInt(args[3]));
			//conf1.setNumReduceTasks(0);

			conf1.setPartitionerClass(map1Partitioner.class);			
			conf1.setOutputValueGroupingComparator(KeywordFreqeGroupingComparator.class);
			conf1.setOutputKeyComparatorClass(CompositeKeyComparator.class); 
			
			//conf.setOutputValueGroupingComparator(KeywordFreqeGroupingComparator.class);
			
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
