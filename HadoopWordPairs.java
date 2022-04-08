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

public class HadoopWordPairs extends Configured implements Tool {
	/*
	A HadoopWordPairs class that splits the input lines received by the map function into sequences of 
	(i) lower-case alphabetic characters (i.e., “words” consisting of chars a-z including
	dashes “-” and under-dashes “_”) and,
	(ii) numeric characters (i.e., “numbers” consisting of digits 0-9 separated by at most one “.”), respectively.
	*/

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text pair = new Text();
		private Text lastWord = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Pattern[] inputRegexes = new Pattern[2];
			inputRegexes[0] = Pattern.compile("^[a-z-_]+$");
			inputRegexes[1] = Pattern.compile("^[-+]?[0-9]+[.]{0,1}[0-9]*$");
			/*
			The regular expression on line 38 captures words that consists of chars a-z including "-"" and "_" only.
			^ Means the word must start with any char from a-z including "-" and "_".
			+ means we are interested in words with atleast one of [a-z_-].
			$ means our word must end with a char from [a-z_-].

			The regular expressions on line 39 captures numeric characters consisting of digits separated by at most one ".".
			^[-+]?[0-9]+ means our character can begin with atmost one plus or negative sign or must begin with a numeric character between 0-9. 
			[.]{0, 1} means there can be atmost one "." between numbers.
			[0-9]*$ The string ends with zero or more numbers between 0 and 9. 
			*/

			String[] splitLine = value.toString().split(" "); // We split each string with " ".

			for (String w : splitLine) { 
				if (lastWord.getLength() > 0) {
				/*
				To explain the logic of our Pairs algorithm, we need to recall we are to capture 
				1) words consisting of chars a-z including _ and -.
				2) numbers consisting of digits 1-9 including one "."

				Our pairs algorithm counts only neighbors that are of the same category. That is it won't pair neighbors if 
				one is a word and one is a number.

				Example; Given the string  "I am in A group of 3 20 year adults with Weight 2.5.5", the reducer of our pairs algorithm would return;
				(am : in, 1), (group : of, 1), (3 : 20, 1),  (year : adults, 1), (adults : with, 1)
				
				We thought it best to go through the entire string and then consider pairs of words that fit into the 2 recently mentioned categories, 
				and since we wanted to output alphabethic words and numeric words separately, we ensured we only considered pairs that are in the same category.
				*/
					for (Pattern inputRegex : inputRegexes){
						if (inputRegex.matcher(w).find() && inputRegex.matcher(lastWord.toString()).find()){
				// For the above if statement; if two pairs are found by the same regular expression, it implies they are of the same type and the pair is counted.
							pair.set(lastWord + ":" + w);
							context.write(pair, one);
						}
					}
				}
				lastWord.set(w);
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
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
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
		long startTime = System.nanoTime();
		Job job = Job.getInstance(new Configuration(), "HadoopWordPairs");
		job.setJarByClass(HadoopWordPairs.class);

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
		int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs(), args);
		System.exit(ret);
	}
}