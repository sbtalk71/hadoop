package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.myorg.WordCount2.WordCountMapper;
import org.myorg.WordCount2.WordCountReducer;

public class WordCountMapperReducerTest {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setup() {

		WordCountMapper maper = new WordCountMapper();
		WordCountReducer reducer = new WordCountReducer();
		mapDriver = MapDriver.newMapDriver(maper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(maper, reducer);
	}
	@Test
	public void testMaper() throws IOException{
		mapDriver.withInput(new LongWritable(), new Text("hello how are you"));
		mapDriver.withOutput(new Text("hello"),new IntWritable(1));
		mapDriver.withOutput(new Text("how"),new IntWritable(1));
		mapDriver.withOutput(new Text("are"),new IntWritable(1));
		mapDriver.withOutput(new Text("you"),new IntWritable(1));
		mapDriver.runTest();
		
	}
	/*public void testReducer() throws IOException{
		reduceDriver.add
	}*/
}
