package a3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LogAnalysisPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text term, IntWritable value, int numReduce) {
		
		if(numReduce == 0) {
			return 0;
		}

		String[] keys = term.toString().split(" ");
		Integer month = Integer.parseInt(keys[0]);
		return (month-1 % numReduce);
	}

}
