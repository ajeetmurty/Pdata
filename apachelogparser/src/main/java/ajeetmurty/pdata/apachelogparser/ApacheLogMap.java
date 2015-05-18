package ajeetmurty.pdata.apachelogparser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ApacheLogMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private final static String apacheLogRegex = "^(\\S+)\\s(.*)$";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Pattern pattern = Pattern.compile(apacheLogRegex);
		Matcher matcher = pattern.matcher("");
		if (matcher.reset(line).matches()) {
			word.set(matcher.group(1));
		} else {
			word.set("unknown-host");
		}
		context.write(word, one);
	}
}