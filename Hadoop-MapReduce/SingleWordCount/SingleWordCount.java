package Cloud;

import java.io.IOException;

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
public class SingleWordCount {
	public static void main(String [] args) throws Exception
	{
		Configuration config=new Configuration();
		String[] files=new GenericOptionsParser(config,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job job=Job.getInstance(config,"wordcount");
		job.setJarByClass(SingleWordCount.class);
		job.setMapperClass(MapForWordCount.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
	}
	/*mapper class for single word count*/
	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] words=line.split(" ");
			for(String word: words )
			{
				Text outputKey = new Text(word.trim());
				IntWritable outputValue = new IntWritable(1);
				context.write(outputKey, outputValue);
			}
		}
	}
     /*Reducer class for Single Word Count*/
	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}
}
