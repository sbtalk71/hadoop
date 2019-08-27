import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AgeStatistics extends Configured implements Tool {


	public static class StatsMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] str = value.toString().split("\t", -2);
			String gender = str[2];
			context.write(new Text(gender), value);
		}
	}

	public static class StatReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		public int max = -1;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			max = -1;

			for (Text value : values) {
				String[] str = value.toString().split("\t", -2);
				//System.out.println(str[3]);
				if (Integer.parseInt(str[3]) > max) {
					max = Integer.parseInt(str[3]);
				}
				context.write(key, new IntWritable(max));
			}

		}
	}

	public static class StatPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReducetasks) {
			String[] str = value.toString().split("\t");
			System.out.println("Partitioner called....");
			int age = Integer.parseInt(str[1]);
			System.out.println(numReducetasks);
			if (numReducetasks == 0) {
				return 0;
			}
			if (age <= 20)
				return 0;
			else if (age > 20 && age <= 40)
				return 1 % numReducetasks;
			else
				return 2 % numReducetasks;
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(AgeStatistics.class);
		job.setJobName("Age Stats");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(StatsMapper.class);
		job.setReducerClass(StatReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(StatPartitioner.class);
		job.setNumReduceTasks(3);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		AgeStatistics as = new AgeStatistics();
		ToolRunner.run(as, args);
	}
}
