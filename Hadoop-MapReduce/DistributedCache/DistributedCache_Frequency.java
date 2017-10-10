package Cloud;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class DistributedCache_Frequency {

	

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Path cacheFile=new Path(files[2]);
		Job job=Job.getInstance(conf,"wordcount");
		job.setJarByClass(DistributedCache_Frequency.class);
		job.setMapperClass(MapForWordCount.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.addCacheFile(cacheFile.toUri());
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
	}
	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		/*Initialize WordCount*/
		public  Set<String> distributedWordCount = new HashSet<String>();
		/*Passing parameters to pre-defined function*/
		@Override
		public void setup(Context con) throws IOException {
		    URI cachedFileURI = con.getCacheFiles()[0];
            Path path = new Path(cachedFileURI.toString());
            readCaheFile(path);
		}
		@Override
		/*mapper class for distributed cache*/
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			
			String line = value.toString();
			String[] words=line.split(" ");
			for(String word: words )
			{
				Text outputKey = new Text(word.trim());
				if(distributedWordCount.contains(outputKey.toString())) {
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}
			}
		}
		/*function that reads cache/second file*/
		public void readCaheFile(Path patternsFile) {
			try {
				BufferedReader secondfile = new BufferedReader(new InputStreamReader(
                        new FileInputStream(patternsFile.toString())));
				String pattern = null;
				while ((pattern = secondfile.readLine()) != null) {
					String[] words = pattern.split(" ");
					for(String word : words) {
						distributedWordCount.add(word.trim());
					}
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
			}
		}
	}
	
	
	/*Reducer class for Distributed Cache*/
	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}


}
