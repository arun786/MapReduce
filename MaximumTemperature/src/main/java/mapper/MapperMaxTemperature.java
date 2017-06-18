/**
 * 
 */
package mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Adwiti
 *
 */
public class MapperMaxTemperature extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int MISSING = 9999;

	/*
	 * input from the file will be as below, this will be in one line
	 * 
	 * 0029029070999991901010106004+64333+023450FM-12+
	 * 000599999V0202701N015919999999N0000001N9-00781+
	 * 99999102001ADDGF108991999999999999999999
	 * 
	 */

	/**
	 * Setup method is first called for initialization, it is called once at the
	 * beginning of the task, it is called once for each mapper.
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}

	/**
	 * Each lines of a file is read and mapped. Each line of the input file is
	 * read and temperature for each year is segregated against the year
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		/*
		 * year is at index 15 to 18 of the line and temperature is at index 88
		 * to 92, we get the value of each line from the second parameter which
		 * is Text.
		 */

		String line = value.toString();
		/* for substring endindex is exclusive */
		String year = line.substring(15, 19);
		int airTemperature = 0;
		/* If temperature consists of + sign, we need to exclude it */
		if (line.charAt(87) == '+') {
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}

		/* quality of the air is at index 92 */
		String quality = line.substring(92, 93);

		if (airTemperature != MISSING && quality.matches("[01456]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}

	@Override
	public void run(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.run(context);
	}

	/**
	 * This is called once for cleaning up at the end of the task
	 */
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}

}
