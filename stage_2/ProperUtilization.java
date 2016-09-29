
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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

class IntArrayWritable extends ArrayWritable { 
	public IntArrayWritable() { 
		super(IntWritable.class); 
	} 
}

public class ProperUtilization {

	public static void main(String[] args) {

		try {
			// MR initialization code
			JobConf conf = new JobConf(ProperUtilization.class);
			conf.setJarByClass(ProperUtilization.class);
			conf.setJobName("Proper Utilization");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapOutputValueClass(IntArrayWritable.class);
			conf.setMapperClass(RoomPropUtilMapper.class);
			conf.setReducerClass(PropUtilReducer.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			// setting the output path
			String outputPath = args[1];
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byRooms"));
			JobClient.runJob(conf);

			conf.setMapperClass(DepartmentPropUtilMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byDepartment"));
			JobClient.runJob(conf);

			conf.setMapperClass(TimePropUtilMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byTime"));
			JobClient.runJob(conf);

			conf.setMapperClass(YearPropUtilMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/byYear"));
			JobClient.runJob(conf);

			conf.setMapperClass(SemesterPropUtilMapper.class);
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/bySemester"));
			JobClient.runJob(conf);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static class RoomPropUtilMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntArrayWritable> /* Output value Type */ {

		private Text word = new Text();

		IntArrayWritable max_current_array = new IntArrayWritable();
		IntWritable max_current[] = new IntWritable[2];

		public void map(LongWritable key, Text value, OutputCollector<Text, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[3].equals("Unknown") || fields[9].equals("") || fields[5].equals("Arr")
					|| fields[5].split(" ")[0].equals("Arr") || fields[6].contains("Unknown")
					|| !StringUtils.isNumeric(fields[9]) || fields[10].equals("0")
					|| Integer.parseInt(fields[10]) < Integer.parseInt(fields[9]))
				return;

			word.set(fields[5]);
			// Percent = Current Students/Max Students * 100;
			max_current[0] = new IntWritable(Integer.parseInt(fields[9]));
			// max_current[0].set(Integer.parseInt(fields[9]));
			max_current[1] = new IntWritable(Integer.parseInt(fields[10]));
			// max_current[1].set(Integer.parseInt(fields[10]));

			// percent =
			// (Integer.parseInt(fields[9])/Integer.parseInt(fields[10]))*100;
			max_current_array.set(max_current);
			output.collect(word, max_current_array);
		}

	}

	public static class DepartmentPropUtilMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntArrayWritable> /* Output value Type */ {

		private Text word = new Text();

		IntArrayWritable max_current_array = new IntArrayWritable();
		IntWritable max_current[] = new IntWritable[2];

		public void map(LongWritable key, Text value, OutputCollector<Text, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[9].equals("") || !StringUtils.isNumeric(fields[9]))
				return;

			word.set(fields[4]);
			// Percent = Current Students/Max Students * 100;
			max_current[0] = new IntWritable(Integer.parseInt(fields[9]));
			// max_current[0].set(Integer.parseInt(fields[9]));
			max_current[1] = new IntWritable(Integer.parseInt(fields[10]));
			// max_current[1].set(Integer.parseInt(fields[10]));

			// percent =
			// (Integer.parseInt(fields[9])/Integer.parseInt(fields[10]))*100;
			max_current_array.set(max_current);
			output.collect(word, max_current_array);
		}

	}

	public static class SemesterPropUtilMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntArrayWritable> /* Output value Type */ {

		private Text word = new Text();

		IntArrayWritable max_current_array = new IntArrayWritable();
		IntWritable max_current[] = new IntWritable[2];

		public void map(LongWritable key, Text value, OutputCollector<Text, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[9].equals("") || !StringUtils.isNumeric(fields[9]))
				return;

			word.set(fields[3].split(" ")[0]);

			// Percent = Current Students/Max Students * 100;
			max_current[0] = new IntWritable(Integer.parseInt(fields[9]));
			// max_current[0].set(Integer.parseInt(fields[9]));
			max_current[1] = new IntWritable(Integer.parseInt(fields[10]));
			// max_current[1].set(Integer.parseInt(fields[10]));

			// percent =
			// (Integer.parseInt(fields[9])/Integer.parseInt(fields[10]))*100;
			max_current_array.set(max_current);
			output.collect(word, max_current_array);
		}

	}

	public static class YearPropUtilMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntArrayWritable> /* Output value Type */ {

		private Text word = new Text();

		IntArrayWritable max_current_array = new IntArrayWritable();
		IntWritable max_current[] = new IntWritable[2];

		public void map(LongWritable key, Text value, OutputCollector<Text, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[9].equals("") || fields[3].split(" ").length <= 0 || !StringUtils.isNumeric(fields[9]))
				return;

			word.set(fields[3].split(" ")[1]);

			// Percent = Current Students/Max Students * 100;
			max_current[0] = new IntWritable(Integer.parseInt(fields[9]));
			// max_current[0].set(Integer.parseInt(fields[9]));
			max_current[1] = new IntWritable(Integer.parseInt(fields[10]));
			// max_current[1].set(Integer.parseInt(fields[10]));

			// percent =
			// (Integer.parseInt(fields[9])/Integer.parseInt(fields[10]))*100;
			max_current_array.set(max_current);
			output.collect(word, max_current_array);
		}

	}

	public static class TimePropUtilMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntArrayWritable> /* Output value Type */ {

		private Text word = new Text();

		IntArrayWritable max_current_array = new IntArrayWritable();
		IntWritable max_current[] = new IntWritable[2];

		public void map(LongWritable key, Text value, OutputCollector<Text, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			String[] fields = value.toString().split(","); // new array of 9
															// elements
			if (fields[9].equals("") || fields[7].contains("Unknown") || !StringUtils.isNumeric(fields[9]))
				return;

			word.set(fields[7]);

			// Percent = Current Students/Max Students * 100;
			max_current[0] = new IntWritable(Integer.parseInt(fields[9]));
			// max_current[0].set(Integer.parseInt(fields[9]));
			max_current[1] = new IntWritable(Integer.parseInt(fields[10]));
			// max_current[1].set(Integer.parseInt(fields[10]));

			// percent =
			// (Integer.parseInt(fields[9])/Integer.parseInt(fields[10]))*100;
			max_current_array.set(max_current);
			output.collect(word, max_current_array);
		}

	}

	public static class PropUtilReducer extends MapReduceBase
			implements Reducer<Text, IntArrayWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntArrayWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			int percent = 0;
			float sum_max = 0;
			float sum_current = 0;

			while (values.hasNext()) {
				IntArrayWritable val = values.next();
				IntWritable i = (IntWritable) val.get()[0];
				IntWritable j = (IntWritable) val.get()[1];
				sum_current += i.get();
				sum_max += j.get();
			}
			if (!(sum_max == 0))
				percent = (int) ((sum_current / sum_max) * 100);
			result.set(percent);
			output.collect(key, result);
		}

	}

}
