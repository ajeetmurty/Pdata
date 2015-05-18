package ajeetmurty.pdata.apachelogparser;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class ApacheLogParser {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		new ApacheLogParser(args[0], args[1]);
	}

	public ApacheLogParser(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		doMapReduce(inputPath, outputPath);
	}
	
	private void doMapReduce(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "ApacheLogParser");
		job.setJarByClass(ApacheLogParser.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(ApacheLogMap.class);
		job.setReducerClass(ApacheLogReduce.class);
		job.setCombinerClass(ApacheLogReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}
}
