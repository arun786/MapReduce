/**
 * 
 */
package Job;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Adwiti
 *
 */
public class JobOnlineAuction {
	public static void main(String[] args) {

	}
}

class MapperOnLineAuction extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		/*
		 * i/p
		 * 
		 * auctionid,bid,bidtime,bidder,bidderrate,openbid,price
		 */

		String bidDetails = value.toString();
		String bidDetail[] = bidDetails.split(",");

		String auctionId = bidDetail[0];
		Integer bid = Integer.parseInt(bidDetail[1]);

		context.write(new Text(auctionId), new IntWritable(bid));

	}
}

class ReducerOnlineAuction extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {

		int maximum = Integer.MIN_VALUE;
		int minimum = Integer.MAX_VALUE;
		double sum = 0;
		int count = 0;

		for (IntWritable value : values) {
			maximum = Math.max(maximum, value.get());
			minimum = Math.min(minimum, value.get());
			sum += value.get();
			count++;
		}

		double average = sum / count;

		context.write(key, new Text("Maximum : " + maximum + " Minimum : " + minimum + " Average : " + average));

	}

}
