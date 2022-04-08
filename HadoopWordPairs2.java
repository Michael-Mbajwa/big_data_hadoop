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

public class HadoopWordPairs2 extends Configured implements Tool {
	/* 
	A HadoopWordPairs class that extracts all pairs of words and/or numbers within a given distance of m such tokens per line.
	*/
	public static int m;  // m, the distance will be provided when we are running the HadoopwordPairs class on the terminal.
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text pair = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitLine = value.toString().split(" ");
			Pattern[] inputRegexes = new Pattern[2];
			inputRegexes[0] = Pattern.compile("^[a-z-_]+$");
    		inputRegexes[1] = Pattern.compile("^[-+]?[0-9]+[.]{0,1}[0-9]*$");
		/*
		The regular expression on line 36 captures words that consists of chars a-z including "-"" and "_" only.
		^ Means the word must start with any char from a-z including "-" and "_".
		+ means we are interested in words with atleast one of [a-z_-].
		$ means our word must end with a char from [a-z_-].

		The regular expressions on line 37 captures numeric characters consisting of digits separated by at most one ".".
		^[-+]?[0-9]+ means our character can begin with atmost one plus or negative sign or must begin with a numeric character between 0-9. 
		[.]{0, 1} means there can be atmost one "." between numbers.
		[0-9]*$ The string ends with zero or more numbers between 0 and 9. 
		*/
			for (int i = 0; i < splitLine.length; i++) {
				Text current_word = new Text(splitLine[i]); // For each loop, we store the current word.
				int j = 0;
				int temp = i;  // a temporary copy of i, the current position.
				while(i < splitLine.length-1 && j < m && temp < splitLine.length-1) {
				/*
				The condition for the while statement are as follows;
				We must never exceed the second to the last string, since at this point, there will be only one potential pair irregardless of the distance m.
				j increases from 0 to m-1. This helps us in checking the exact number of potential pairs within distance m.
				temp helps in getting the position of the pairs within the specified distance m.
				*/
					Text pair_word = new Text(splitLine[i+j+1]); // i+j+1 helps the next pair after each while loop
					for (Pattern inputRegex : inputRegexes){
						if (inputRegex.matcher(current_word.toString()).find() && inputRegex.matcher(pair_word.toString()).find()){
							pair.set(current_word.toString() + ":" + pair_word.toString());
							context.write(pair, one);
						}
					}
					j++;
					temp++;
					/*
					Summary of how the algorithm works;
					For example, our input is 'i study In 1 luxembourg' and we are at position 0, with current_word "i".
					Assuming m=4, the algorithm would return (i : study, 1), (i : luxembourg, 1). 
					(i : In, 1) will not be returned because It does not satisfy our criteria of only lowercases words.
					(i : 1, 1) will not be returned because they are both different categories and will make splitting the outoputs impossible.
					*/
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
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
			Pattern pattern = Pattern.compile("^[a-z]+");
			Matcher matcher = pattern.matcher(key.toString());
			boolean matchFound = matcher.find();
			if (matchFound) {
				return 1 % numReduceTasks;
			}
			else {
				return 2 % numReduceTasks;
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		m = Integer.parseInt(args[2]);  // m, the distance will be the last variable provided on the terminal when running the jar file on the command terminal.
		long startTime = System.nanoTime();
		Job job = Job.getInstance(new Configuration(), "HadoopWordPairs2");
		job.setJarByClass(HadoopWordPairs2.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// set partitioner statement
		job.setPartitionerClass(Partition.class);
		job.setNumReduceTasks(2);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		long endTime   = System.nanoTime();
		long totalTime = endTime - startTime;
		System.out.println("Total time (s) :" + (double)(totalTime)/1000000000);  // returns computation time.
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs2(), args);
		System.exit(ret);
	}
}