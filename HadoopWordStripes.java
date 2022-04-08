import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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

public class HadoopWordStripes extends Configured implements Tool {
	/*
	A HadoopWordStripes class that splits the input lines received by the map function into sequences of 
	(i) lower-case alphabetic characters (i.e., “words” consisting of chars a-z including
	dashes “-” and under-dashes “_”) and,
	(ii) numeric characters (i.e., “numbers” consisting of digits 0-9 separated by at most one “.”), respectively.
	*/

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
		private final static IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] splitLine = value.toString().split(" ");
			Pattern[] inputRegexes = new Pattern[2];
			inputRegexes[0] = Pattern.compile("^[a-z-_]+$");
			inputRegexes[1] = Pattern.compile("^[-+.]?[0-9]+[.]{0,1}[0-9]*$");
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

			for (int i = 0; i < splitLine.length; i++) {

				for (Pattern inputRegex : inputRegexes) {
					
					Text current_word = new Text(splitLine[i]);  // Current word
					
					if (inputRegex.matcher(current_word.toString()).find()) { 
					/*
					The above if statement means we only work with strings that fall into one of the two categories;
					1) Keys that contain only numbers and one decimal point.
					2) Keys that contain alphabeths between [a-z] and any of "_"  and "-".
					*/

						Pattern[] input_Regexes = new Pattern[3];
						input_Regexes[0] = Pattern.compile("^[a-z-_]+$");
						input_Regexes[1] = Pattern.compile("^[-+]?[0-9]+[.]{0,1}[0-9]+$");

						
						MapWritable map = new MapWritable();
						String w;
		
						if (i > 0) {
							w = splitLine[i - 1];
							for (Pattern input_Regex : input_Regexes) {
								if (input_Regex.matcher(current_word.toString()).find() && input_Regex.matcher(w).find()) {
								/*
								If the previous word is of the same type with the current word (that is they are both numeric with a single "." or only 
								contain alphabeths), a stripe is created. Otherwise nothing is done.
								*/
									stripe(w, map);
								}
							}
						}
		
						if (i < splitLine.length - 1) {
							w = splitLine[i + 1];
							for (Pattern input_Regex : input_Regexes) {
								if (input_Regex.matcher(current_word.toString()).find() && input_Regex.matcher(w).find()) {
								/*
								If the latter (i.e. next) word is of the same type with the current word (that is they are both numeric or they both 
								only contain alphabeths), a stripe is created. Otherwise nothing is done.
								*/
									stripe(w, map);
								}
							}
						}
		
						context.write(current_word, map);
					/*
					Our stripes mapper algorithm works in this manner, it goes through each word in the text and if it falls into one of our two categories,
					we study it's neighbors at a distance of one in both directions. Because we are to later split the file into two categories, we count only
					neighbors that fall under the same category with the current word in the loop. Let's say for example the string is "i am 20" and the current
					word is "am". Our stripes mapper will return (am, {i: 1}). "20" won't be considered although it falls into one of our two categories because
					it is not of the same type with "am". 
					Another example; "i Am a boy". Let's say the current word is "a". Our stripes mapper algorithm would return (a, {boy: 1}). "Am" won't be 
					considered because it does not fall into any of our two categories.
					*/
					}
			}
		}
		}

		public static void stripe(String w, MapWritable map) {
			LongWritable count = new LongWritable(0);

			if (map.containsKey(new Text(w))) {
				count = (LongWritable) map.get(new Text(w));
				map.remove(new Text(w));
			}

			count = new LongWritable(count.get() + one.get());
			map.put(new Text(w), count);
		}

	}

	public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {

		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			MapWritable stripe = new MapWritable();

			for (MapWritable localStripe : values) {
				Iterator entries = localStripe.entrySet().iterator();

				while (entries.hasNext()) {
					java.util.Map.Entry thisEntry = (java.util.Map.Entry) entries.next();
					Text keyNeighbour = (Text) thisEntry.getKey();
					LongWritable value = (LongWritable) thisEntry.getValue();
					globalStripe(keyNeighbour, value, stripe);
				}
			}

			context.write(key, stripe);
		}

		public static void globalStripe(Text key, LongWritable value, MapWritable map) {
			LongWritable sum = new LongWritable(0);

			if (map.containsKey(key)) {
				sum = (LongWritable) map.get(key);
				map.remove(key);
			}

			sum = new LongWritable(sum.get() + value.get());
			map.put(key, sum);
		}
	}

	// Partitioner Class
	public static class Partition extends Partitioner <Text, MapWritable> {
		@Override
		public int getPartition(Text key, MapWritable value, int numReduceTasks)
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
		long startTime = System.nanoTime();
		Job job = Job.getInstance(new Configuration(), "HadoopWordStripes");
		job.setJarByClass(HadoopWordStripes.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// set partitioner statement
		job.setPartitionerClass(Partition.class);
		job.setNumReduceTasks(2);  // This is what ensures we have two different outputs.

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		long endTime   = System.nanoTime();
		long totalTime = endTime - startTime;
		System.out.println("Total time (s) :" + (double)(totalTime)/1000000000);  // Counts compilation time.
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordStripes(), args);
		System.exit(ret);
	}
}
