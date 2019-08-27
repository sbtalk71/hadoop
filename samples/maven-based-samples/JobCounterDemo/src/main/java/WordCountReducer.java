import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			String token = key.toString();
			if( StringUtils.startsWithDigit(token) ){
				context.getCounter(WordsNature.STARTS_WITH_DIGIT).increment(1);
			}
			else if( StringUtils.startsWithLetter(token) ){
				context.getCounter(WordsNature.STARTS_WITH_LETTER).increment(1);
			}
			context.getCounter(WordsNature.ALL).increment(1);
			for (IntWritable value : values) {
				sum = sum + value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}