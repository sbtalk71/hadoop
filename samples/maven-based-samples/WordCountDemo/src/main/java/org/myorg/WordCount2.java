package org.myorg;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount2 {
	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("Key = "+key.get());
			final IntWritable one = new IntWritable(1);
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line);
			while (tokens.hasMoreTokens()) {
				String word = tokens.nextToken();
				context.write(new Text(word), one);

			}
		}
	}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum = sum + value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class WordCountDriver extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			Job job = new Job();
			job.setJarByClass(WordCountDriver.class);
			job.setJobName("Word Count 2");
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		}
	}

	public static void main(String[] args) throws Exception {
		WordCountDriver wd= new WordCountDriver();
		ToolRunner.run(wd, args);
	}

}
