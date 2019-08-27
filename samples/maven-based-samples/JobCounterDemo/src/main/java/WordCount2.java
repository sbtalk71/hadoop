

import org.apache.hadoop.util.ToolRunner;

public class WordCount2 {
	public static void main(String[] args) throws Exception {
		WordCountDriver wd= new WordCountDriver();
		ToolRunner.run(wd, args);
	}

}
