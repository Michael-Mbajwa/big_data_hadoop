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

public class HadoopWordStripes2 extends Configured implements Tool {
	/* 
	A HadoopWordStripes class that extracts all pairs of words and/or numbers within a given distance of m such tokens per line.
	*/
	public static int m;  // m, the distance will be provided when we are running the HadoopwordPairs class on the terminal.

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
		private final static IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] splitLine = value.toString().split(" ");
			Pattern[] inputRegexes = new Pattern[2];
			inputRegexes[0] = Pattern.compile("^[a-z-_]+$");
    		inputRegexes[1] = Pattern.compile("^[-+]?[0-9]+[.]{0,1}[0-9]*$");
		/*
		The regular expression on line 37 captures words that consists of chars a-z including "-"" and "_" only.
		^ Means the word must start with any char from a-z including "-" and "_".
		+ means we are interested in words with atleast one of [a-z_-].
		$ means our word must end with a char from [a-z_-].

		The regular expressions on line 38 captures numeric characters consisting of digits separated by at most one ".".
		^[-+]?[0-9]+ means our character can begin with atmost one plus or negative sign or must begin with a numeric character between 0-9. 
		[.]{0, 1} means there can be atmost one "." between numbers.
		[0-9]*$ The string ends with zero or more numbers between 0 and 9. 
		*/
			for (int i = 0; i < splitLine.length; i++) {
				MapWritable map = new MapWritable();
			/*
			SUMMARY OF THIS STRIPES ALGORITHM
			The HadoopWordStripes example provided in this lecture works in the following way;
			For each current word, if it exists, the algorithm picks the adjacent word to the left and right of the current word. For example; For a given string "i am good",
			if the current word is "am" the stripes mapper returns (am, {i: 1, good: 1}). We assume this is for a distance m=1. 

			With this understanding, for each word at position i and any distance m, our algorithm picks adjacent words within m distance from left and right if it exists. Our algorithm is greedy
			in the sense that if m=5 and words by left or right are not up to 5, the algorithm still picks all the words irregardless.

			Since our algorithm only considers words based on these two criteria; 
			1) Numbers with atmost one "."
			2) Words with alphabeths between [a-z] also including "_"  and "-"

			For each word, the algorithm ensures the stripes created are of the same type. We won't create stripes between alphabeths and numbers. This will enable making splitting the output easier.
			*/
				// FOR THE RIGHT STRIPES OF EACH WORD
				Text current_word = new Text(splitLine[i]); // For each loop, we store the current word.
				int j_right = 0;
				int temp_right = i;  // a temporary copy of i, the current position.
				while(i < splitLine.length-1 && j_right < m && temp_right < splitLine.length-1) {
				/*
				The condition for the while statement are as follows;
				We must never exceed the second to the last string, since at this point, there will be only one potential pair irregardless of the distance m.
				j_right increases from 0 to m-1. This helps us in checking the exact number of potential pairs within distance m.
				temp_right helps in getting the position of the pairs within the specified distance m.
				*/
					Text pairWordRight = new Text(splitLine[i+j_right+1]); // i+j_right+1 helps the next pair after each while loop
					for (Pattern inputRegex : inputRegexes){
						if (inputRegex.matcher(current_word.toString()).find() && inputRegex.matcher(pairWordRight.toString()).find()){
							stripe(pairWordRight.toString(), map);
						}
					}
					j_right++;
					temp_right++;
					/*
					Summary of how the algorithm works;
					For example, our input is 'i study In 1 luxembourg' and we are at position 0, with current_word "i".
					Assuming m=4, the algorithm would return (i : study, 1), (i : luxembourg, 1). 
					(i : In, 1) will not be returned because It does not satisfy our criteria of only lowercases words.
					(i : 1, 1) will not be returned because they are both different categories and will make splitting the outoputs impossible.
					*/
				}

				// FOR THE LEFT STRIPES OF EACH WORD
				int j_left = i;
				int temp_left = 0;
				while (j_left > 0 && temp_left < m) {
					Text pairWordLeft = new Text(splitLine[j_left-1]);
					for (Pattern inputRegex : inputRegexes){
					if (inputRegex.matcher(current_word.toString()).find() && inputRegex.matcher(pairWordLeft.toString()).find()){
						stripe(pairWordLeft.toString(), map);
					}
				}
					j_left--;
					temp_left++;
				}
				// writes only current words that fall into one of the two categories.
				for (Pattern inputRegex : inputRegexes){
					if (inputRegex.matcher(current_word.toString()).find()){
					context.write(current_word, map);}	}
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
		m = Integer.parseInt(args[2]);  // m, the distance will be the last variable provided on the terminal when running the jar file on the command terminal.
		long startTime = System.nanoTime();
		Job job = Job.getInstance(new Configuration(), "HadoopWordStripes2");
		job.setJarByClass(HadoopWordStripes2.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

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
		int ret = ToolRunner.run(new Configuration(), new HadoopWordStripes2(), args);
		System.exit(ret);
	}
}
