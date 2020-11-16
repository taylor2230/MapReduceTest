package a3;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	public void reduce(Text term, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {
		int count = 0;
		Iterator<IntWritable> iterator = ones.iterator();
		
		while(iterator.hasNext()) {
			count += iterator.next().get();
		}
		IntWritable output = new IntWritable(count);
		context.write(term, output);
	}

}
