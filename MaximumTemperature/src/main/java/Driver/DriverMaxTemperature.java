/**
 * 
 */
package Driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

import mapper.MapperMaxTemperature;
import reducer.ReducerMaxTemperature;

/**
 * @author Adwiti
 *
 */
public class DriverMaxTemperature {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Parameters not valid");
			System.exit(-1);
		}

		try {

			Configuration config = new Configuration();
			Job job = Job.getInstance(config, "Max Temperature");

			/* Set the jar file */
			job.setJarByClass(DriverMaxTemperature.class);

			/* set the mapper and reducer */
			job.setMapperClass(MapperMaxTemperature.class);
			job.setReducerClass(ReducerMaxTemperature.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			try {
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			} catch (ClassNotFoundException | InterruptedException e) {
				e.printStackTrace();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
