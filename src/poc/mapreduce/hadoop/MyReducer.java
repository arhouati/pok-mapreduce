package poc.mapreduce.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends  Reducer<Text, LongWritable, Text, LongWritable>{

	public MyReducer() {
    }
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		
		System.out.println("poc : Reduce : process");
		
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
