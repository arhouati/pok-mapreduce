package pok.mapreduce.hadoop;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import pok.algorithm.DataMining;

public class MyMapper extends TableMapper<Text, LongWritable> {

	private final LongWritable ONE = new LongWritable(1);
	
    public MyMapper() {
    }

	@Override
	protected void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {
		
		System.out.println("poc : Map : process row");
		
		byte[] textByte = columns.getValue("Comment".getBytes(), "Text".getBytes());
	    String text = Bytes.toString(textByte);
	    
		byte[] langByte = columns.getValue("Comment".getBytes(), "Lang".getBytes());
	    String lang = Bytes.toString(langByte).trim();

	    int score = 0;
	    
		if( "fr".equals(lang) ){
			score = DataMining.process( text , lang);
			System.out.println("# text row : " + row );
		}
		
		if (score > 0) {
			context.write(new Text("positive"), ONE);
		}else if (score < 0) {
			context.write(new Text("negative"), ONE);
		}else if (score == 0){
			context.write(new Text("neutral"), ONE);
		}
	}
}
