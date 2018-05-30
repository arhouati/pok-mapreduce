package pok.mapreduce.hadoop;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import pok.algorithm.DataMining;

public class MapperSumSentiment extends TableMapper<Text, LongWritable> {

	private final LongWritable One = new LongWritable(1);

	private final FileWriter fstream;
	private final BufferedWriter out;
	
    public MapperSumSentiment() throws IOException {
    	
		fstream = new FileWriter("output/sentences.txt", true); //true tells to append data.
	    out = new BufferedWriter(fstream);

    }

	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		System.out.println("poc : Map Sum Sentiment : process row");
		
		context.write(new Text("positive"), One);

		int score = Integer.parseInt( value.toString() );
		
		if ( score > 0) {
			context.write(new Text("positive"), One);
			
		}else if (score < 0) {
			context.write(new Text("negative"), One);
			
		}else if (score == 0){
			context.write(new Text("neutral"), One);
			
		}
		
	}
}
