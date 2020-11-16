package a3;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogAnalysisMapper extends Mapper<Object, Text, Text, IntWritable>  {

	private static final HashMap<String, Integer> monthNums = new HashMap<String, Integer>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	{
		put("JAN", 1);
		put("FEB", 2);
		put("MAR", 3);
		put("APR", 4);
		put("MAY", 5);
		put("JUN", 6);
		put("JUL", 7);
		put("AUG", 8);
		put("SEP", 9);
		put("OCT", 10);
		put("NOV", 11);
		put("DEC", 12);
	}};
	
	/*
	 * map function is going through each word with hasMoreTokens()
	 * Change it to not parse through each word and only target col1 as the wordOut
	 * */
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(" ");
		if(line.length > 1) {
			String[] dateAccess = line[3].split("/");
			Integer monthNumber = monthNums.get(dateAccess[1].toUpperCase());
			Text text = new Text();
			IntWritable one = new IntWritable(1);
			text.set(monthNumber.toString() + " " + line[0]);
			context.write(text, one);
		}
	}

}
