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

public class MapperDistinctUser extends TableMapper<Text, LongWritable> {

	private final LongWritable Positive = new LongWritable(1);
	private final LongWritable Negative = new LongWritable(-1);
	private final LongWritable Neutral = new LongWritable(0);

	private final FileWriter fstream;
	private final BufferedWriter out;
	
    public MapperDistinctUser() throws IOException {
    	
		fstream = new FileWriter("output/sentences.txt", true); //true tells to append data.
	    out = new BufferedWriter(fstream);

    }

	@Override
	protected void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {
		
		System.out.println("poc : Map Distinct User : process row");
		
		byte[] textByte = columns.getValue("Comment".getBytes(), "Text".getBytes());
	    String text = Bytes.toString(textByte);
	    
		byte[] langByte = columns.getValue("Comment".getBytes(), "Lang".getBytes());
	    String lang = Bytes.toString(langByte).trim();
	    
		byte[] userByte = columns.getValue("User".getBytes(), "Identifiant".getBytes());
	    String user = Bytes.toString(userByte).trim().replaceAll("\\s","");

	    int score = 0;
	    
		if( "fr".equals(lang) && !"".equals(text)){
			score = DataMining.process( text , lang);
		}
		
		if (score > 0) {
			context.write(new Text(user), Positive);
	        
		}else if (score < 0) {
			context.write(new Text(user), Negative);
			
		}else if (score == 0){
			context.write(new Text(user), Neutral);
			
		}
		
	}
}
