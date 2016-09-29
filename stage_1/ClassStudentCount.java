/* Project 2 Part 1 - Classroom Student Count
Abinash Behera - UBIT ID - abinashb@buffalo.edu, Person No. - 50169804
Lalith Vikram Natarajan - UBIT ID - lalithvi@buffalo.edu, Person No. - 50169243
*/

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ClassStudentCount {
	public static void main(String[] args) {

		try {
			// MR initialization code
			JobConf conf = new JobConf(ClassStudentCount.class);
			conf.setJarByClass(ClassStudentCount.class);
			conf.setJobName("roomcount");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);
			conf.setMapperClass(CustomMapper.class);
			conf.setCombinerClass(CustomReducer.class);
			conf.setReducerClass(CustomReducer.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//Custom Mapper Class
	public static class CustomMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		private final static IntWritable intwritableObject = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String wordInput = itr.nextToken();
				String[] eachColumn = wordInput.split(",");
				//Data Cleaning
				if (eachColumn[1].equals("Unknown") || eachColumn[7].equals("") || eachColumn[2].equals("Arr")
						|| eachColumn[2].split(" ")[0].equals("Arr") || eachColumn[2].contains("Unknown")
						|| !StringUtils.isNumeric(eachColumn[7]))
					return;
				//Only  the first word of the location needed
				String class_semester = eachColumn[2].split(" ")[0] + "_" + eachColumn[1];
				int currStudents = Integer.parseInt(eachColumn[7]);
				word.set(class_semester);
				intwritableObject.set(currStudents);
				output.collect(word, intwritableObject);

			}
		}

	}
	
	public static class CustomReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}

	}

}
