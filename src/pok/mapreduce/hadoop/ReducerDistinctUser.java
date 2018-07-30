package pok.mapreduce.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerDistinctUser extends  Reducer<Text, LongWritable, Text, LongWritable>{

	public ReducerDistinctUser() {
    }
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		
		System.out.println("poc : Reduce Distinct User : process");
		
		// Add up all of the page views for this hour
        long sum = 0;
        for( LongWritable count : values )
        {
            sum += count.get();
        }

        // Write out the current hour and the sum
        context.write( key, new LongWritable( sum ) );
	}
	
}
