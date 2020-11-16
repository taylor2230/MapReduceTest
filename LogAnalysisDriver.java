package a3;
import a3.LogAnalysisMapper;
import a3.LogAnalysisReducer;
import a3.LogAnalysisCombiner;
import a3.LogAnalysisPartitioner;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class LogAnalysisDriver {

	/*
	 * Interacts the with HDFS to set the job information based on the created classes
	 * */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2) {
			System.err.println("Requires: input_file output_directory");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "IP Count");
		job.setJarByClass(LogAnalysisDriver.class);
		job.setMapperClass(LogAnalysisMapper.class);
		job.setCombinerClass(LogAnalysisCombiner.class);
		job.setPartitionerClass(LogAnalysisPartitioner.class);
		job.setNumReduceTasks(12);
		job.setReducerClass(LogAnalysisReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		boolean result = job.waitForCompletion(true);
		
		if(result) {
			System.exit(0);
		} else {
			System.exit(1);
		}
		
	}
}