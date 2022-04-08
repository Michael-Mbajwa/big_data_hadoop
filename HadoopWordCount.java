import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Partitioner;


public class HadoopWordCount extends Configured implements Tool {
	/*
	A HadoopWordCount class that splits the input lines received by the map function into sequences of 
	(i) lower-case alphabetic characters (i.e., “words” consisting of chars a-z including
	dashes “-” and under-dashes “_”) and,
	(ii) numeric characters (i.e., “numbers” consisting of digits 0-9 separated by at most one “.”), respectively.
	*/

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);  // stores the count for each word.
		private Text word = new Text();  // Variable for storing each new word encountered.

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Pattern[] inputRegexes = new Pattern[2];  // A list initialized to contain two different regular expressions.
			inputRegexes[0] = Pattern.compile("^[a-z-_]+$");
			inputRegexes[1] = Pattern.compile("^[-+]?[0-9]+[.]{0,1}[0-9]*$");
			/*
			The regular expression on line 39 captures words that consists of chars a-z including "-"" and "_" only.
			^ Means the word must start with any char from a-z including "-" and "_".
			+ means we are interested in words with atleast one of [a-z_-].
			$ means our word must end with a char from [a-z_-].

			The regular expressions on line 40 captures numeric characters consisting of digits separated by at most one ".".
			^[-+]?[0-9]+ means our character can begin with atmost one plus or negative sign or must begin with a numeric character between 0-9. 
			[.]{0, 1} means there can be atmost one "." between numbers.
			[0-9]*$ The string ends with zero or more numbers between 0 and 9. 
			*/
			String[] splitLine = value.toString().split(" ");  // We split each string with " ".
			
			for (String w : splitLine) {
				for (Pattern inputRegex : inputRegexes) {  // We loop through each regular expression
					if (inputRegex.matcher(w).find()) {  // if any of the regular expression matches the string, the mapper gives it a count.
						word.set(w);
						context.write(word, one); 
			}
		}
	}
}
	}


	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;

			for (IntWritable value : values)
				sum += value.get();

			context.write(key, new IntWritable(sum));
		}
	}

	// Partitioner Class
	public static class Partition extends Partitioner <Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks)
		{
			/*
			The goal of our partitioner here is to separate our dataset based on two conditions:
			1) Keys that contain only numbers with atmost ".".
			2) Keys that contains alphabeths between [a-z] and "_"  and "-".

			The idea of our partitioner here is that if the key sent by the mapper contains alphabeths between [a-z] it sends
			it to one particular reducer otherwise (in the case of numbers) it sends to another reducer.
			*/
			Pattern pattern = Pattern.compile("[a-z]+");
			Matcher matcher = pattern.matcher(key.toString());
			boolean matchFound = matcher.find();
			if (matchFound) {  // if the key matches ^[a-z-_]+
				return 1 % numReduceTasks;
			}
			else {  // if the key does not match ^[a-z-_]+
				return 2 % numReduceTasks;
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		long startTime = System.nanoTime();
		Job job = Job.getInstance(new Configuration(), "HadoopWordCount");
		job.setJarByClass(HadoopWordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// set partitioner statement
		job.setPartitionerClass(Partition.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(2);  // Helps the partitioner.

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		long endTime   = System.nanoTime();
		long totalTime = endTime - startTime;
		System.out.println("Total time (s) :" + (double)(totalTime)/1000000000);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordCount(), args);
		System.exit(ret);
	}
}