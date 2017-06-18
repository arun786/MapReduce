/**
 * 
 */
package reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Adwiti
 *
 */
public class ReducerMaxTemperature extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		/*
		 * input to the reducer from the mapper will be in the form of key value
		 * pair
		 * 
		 * 1950,[10,20] 1951,[23,34]
		 */

		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}

		context.write(new Text(key), new IntWritable(maxValue));
	}

}
