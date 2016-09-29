
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

/**
 * Hello world!
 *
 */
public class Statistics {
	public static void main(String[] args) {
		try {
			// MR initialization code
			JobConf conf = new JobConf(Statistics.class);
			conf.setJarByClass(Statistics.class);
			conf.setJobName("Statistics");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			// conf.setMapOutputKeyClass(theClass);
			conf.setMapOutputValueClass(IntWritable.class);
			conf.setMapperClass(StatisticsRoomsMapper.class);
			// Error: java.io.IOException: wrong value class: class
			// org.apache.hadoop.io.Text is not class
			// org.apache.hadoop.io.IntWritable
			// conf.setCombinerClass(CustomReducer.class);// TODO fInd out why
			conf.setReducerClass(StatisticsReducer.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			// setting the output path
			String outputPath = args[1];
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byRooms"));
			JobClient.runJob(conf);

			conf.setMapperClass(StatisticsDepartmentMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byDepartment"));
			JobClient.runJob(conf);

			conf.setMapperClass(StatisticsTimeMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byTime"));
			JobClient.runJob(conf);

			conf.setMapperClass(StatisticsYearMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byYear"));
			JobClient.runJob(conf);

			conf.setMapperClass(StatisticsSemesterMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/bySemester"));
			JobClient.runJob(conf);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class StatisticsRoomsMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		private final static IntWritable intwritableObject = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[9].equals("") || fields[5].contains("Arr") || fields[5].contains("ARR")
					|| !StringUtils.isNumeric(fields[9]))
				return;

			word.set(fields[5]);
			intwritableObject.set(Integer.parseInt(fields[9]));
			// System.out.println(wordInput);
			output.collect(word, intwritableObject);

		}

	}

	public static class StatisticsDepartmentMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		private final static IntWritable intwritableObject = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[9].equals("") || !StringUtils.isNumeric(fields[9]))
				return;

			word.set(fields[4]);
			intwritableObject.set(Integer.parseInt(fields[9]));
			// System.out.println(wordInput);
			output.collect(word, intwritableObject);

		}

	}

	public static class StatisticsSemesterMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		private final static IntWritable intwritableObject = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[9].equals("") || !StringUtils.isNumeric(fields[9]))
				return;

			word.set(fields[3].split(" ")[0]);
			intwritableObject.set(Integer.parseInt(fields[9]));
			// System.out.println(wordInput);
			output.collect(word, intwritableObject);

		}

	}

	public static class StatisticsYearMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		private final static IntWritable intwritableObject = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[9].equals("") || fields[3].split(" ").length <= 0 || !StringUtils.isNumeric(fields[9]))
				return;

			word.set(fields[3].split(" ")[1]);
			intwritableObject.set(Integer.parseInt(fields[9]));
			// System.out.println(wordInput);
			output.collect(word, intwritableObject);

		}

	}

	public static class StatisticsTimeMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		private final static IntWritable intwritableObject = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[9].equals("") || fields[7].contains("Unknown") || !StringUtils.isNumeric(fields[9]))
				return;

			word.set(fields[7]);
			intwritableObject.set(Integer.parseInt(fields[9]));
			// System.out.println(wordInput);
			output.collect(word, intwritableObject);

		}

	}

	public static class StatisticsReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			float sum = 0;
			float count = 0;
			float average = 0;
			float median = 0;
			List<Integer> contents = new ArrayList<Integer>();
			while (values.hasNext()) {
				int currNumber = values.next().get();
				contents.add(currNumber);
				sum += currNumber;
				count++;
			}
			average = sum / count;
			// calculating the median
			Collections.sort(contents);
			if (contents.size() > 0) {
				if (contents.size() % 2 == 0) {
					median = (contents.get(contents.size() / 2) + contents.get(contents.size() / 2 - 1)) / 2;
				} else {
					median = contents.get(contents.size() / 2);
				}
			}
			// calculating the standard deviation - SD = sqrt(summation(x -
			// mean)/n)
			float differenceOfNumAndAvg = 0;
			for (Integer number : contents) {
				differenceOfNumAndAvg += Math.pow((number - average), 2.0);
			}
			float sd = (float) Math.sqrt((differenceOfNumAndAvg / contents.size()));

			output.collect(key, new Text(String.format("%.2f", average) + "\t" + String.format("%.2f", median) + "\t"
					+ String.format("%.2f", sd)));
		}

	}

}
