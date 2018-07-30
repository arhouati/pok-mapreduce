package pok.mapreduce.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperSumSentiment extends Mapper<Text, Text, Text, LongWritable>  {

	private final LongWritable One = new LongWritable(1);

	//private final FileWriter fstream;
	//private final BufferedWriter out;
	
    public MapperSumSentiment() {
   
		//fstream = new FileWriter("output/sentences.txt", true); //true tells to append data.
	    //out = new BufferedWriter(fstream);

    }

	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		System.out.println("poc : Map Sum Sentiment : process row");
	
		float score = Float.parseFloat( value.toString() );
		
		if ( score > 0) {
			context.write(new Text("positive"), One);
			
		}else if (score < 0) {
			context.write(new Text("negative"), One);
			
		}else if (score == 0){
			context.write(new Text("neutral"), One);
			
		}
		
	}
}
