import java.io.IOException;

import java.util.Iterator;

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

public class Busyness {
	public static void main(String[] args) {
		try {
			// MR initialization code
			JobConf conf = new JobConf(Busyness.class);
			conf.setJarByClass(Busyness.class);
			conf.setJobName("Busyness");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			// conf.setMapOutputKeyClass(theClass);
			conf.setMapOutputValueClass(IntWritable.class);
			conf.setMapperClass(BusynessSemesterMapper.class);
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
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/bySemester"));
			JobClient.runJob(conf);

			conf.setMapperClass(BusynessYearMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byYear"));
			JobClient.runJob(conf);

			conf.setMapperClass(BusynessTimeMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byTime"));
			JobClient.runJob(conf);

			conf.setMapperClass(BusynessBuildingMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byBuilding"));
			JobClient.runJob(conf);

			conf.setMapperClass(BusynessDepartmentMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byDepartment"));
			JobClient.runJob(conf);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class BusynessSemesterMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		private final static IntWritable intwritableObject = new IntWritable(1);
		private Text word = new Text();

		IntWritable day_counter_map = new IntWritable(1);

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[3].equals("Unknown") || fields[9].equals("") || fields[5].equals("Arr")
					|| fields[5].split(" ")[0].equals("Arr") || fields[6].contains("Unknown")
					|| !StringUtils.isNumeric(fields[9]) || fields[10].equals("0") || fields[6].equals("UNKWN")
					|| fields[6].equals("ARR"))
				return;
			
			String semester = fields[3].split(" ")[0];
			word.set(semester);
			int day_counter = fields[6].split("").length;
			day_counter_map.set(day_counter);
			// mos.write("semester_wise",word,day_counter_map);
			output.collect(word, day_counter_map);
		}

	}

	public static class BusynessYearMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		IntWritable day_counter_map = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[3].equals("Unknown") || fields[9].equals("") || fields[5].equals("Arr")
					|| fields[5].split(" ")[0].equals("Arr") || fields[6].contains("Unknown")
					|| !StringUtils.isNumeric(fields[9]) || fields[10].equals("0") || fields[6].equals("UNKWN")
					|| fields[6].equals("ARR"))
				return;

			String year = fields[3].split(" ")[1];
			word.set(year);
			int day_counter = fields[6].split("").length;
			day_counter_map.set(day_counter);
			// mos.write("semester_wise",word,day_counter_map);
			output.collect(word, day_counter_map);
		}

	}

	public static class BusynessDepartmentMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		IntWritable day_counter_map = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[3].equals("Unknown") || fields[9].equals("") || fields[5].equals("Arr")
					|| fields[5].split(" ")[0].equals("Arr") || fields[6].contains("Unknown")
					|| !StringUtils.isNumeric(fields[9]) || fields[10].equals("0") || fields[4].equals("")
					|| fields[6].equals("UNKWN") || fields[6].equals("ARR"))
				return;

			String department = fields[4];
			word.set(department);
			int day_counter = fields[6].split("").length;
			day_counter_map.set(day_counter);
			// mos.write("semester_wise",word,day_counter_map);
			output.collect(word, day_counter_map);
		}

	}

	public static class BusynessBuildingMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		IntWritable day_counter_map = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[3].equals("Unknown") || fields[9].equals("") || fields[5].equals("Arr")
					|| fields[5].equals("Unknown") || fields[5].split(" ")[0].equals("Arr")
					|| fields[6].contains("Unknown") || !StringUtils.isNumeric(fields[9]) || fields[10].equals("0")
					|| fields[6].equals("UNKWN") || fields[6].equals("ARR"))
				return;

			String building = fields[5].split(" ")[0];
			word.set(building);
			int day_counter = fields[6].split("").length;
			day_counter_map.set(day_counter);
			output.collect(word, day_counter_map);
		}

	}

	public static class BusynessTimeMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */ {

		private IntWritable day_counter_map = new IntWritable(1);

		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[3].equals("Unknown") || fields[9].equals("") || fields[5].equals("Arr")
					|| fields[5].equals("Unknown") || fields[5].split(" ")[0].equals("Arr")
					|| fields[6].contains("Unknown") || !StringUtils.isNumeric(fields[9]) || fields[10].equals("0")
					|| fields[6].equals("UNKWN") || fields[6].equals("ARR")
					)
				return;

			String time = fields[7];
			word.set(time);
			int day_counter = fields[6].split("").length;
			day_counter_map.set(day_counter);
			output.collect(word, day_counter_map);
		}

	}

	public static class StatisticsReducer extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, IntWritable> {

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
