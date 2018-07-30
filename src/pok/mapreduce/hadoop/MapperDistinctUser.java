package pok.mapreduce.hadoop;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import pok.algorithm.DataMining;
import pok.mapreduce.txt.tools.PreProcessador;

public class MapperDistinctUser extends TableMapper<Text, LongWritable> {


	private final LongWritable Positive = new LongWritable(1);
	private final LongWritable Negative = new LongWritable(-1);
	private final LongWritable Neutral = new LongWritable(0);

	/*
 	private final FileWriter fstreamPositive;
	private final FileWriter fstreamNegative;
	private final FileWriter fstreamNeutral;

	private final BufferedWriter outPositve;
	private final BufferedWriter outNegative;
	private final BufferedWriter outNeutral;
	*/
	
    public MapperDistinctUser() {
    
    	/*
    	String candidat = "Macron";
    	fstreamPositive = new FileWriter("output/"+candidat + "_positive_tweets.txt", true); 
    	fstreamNegative = new FileWriter("output/"+candidat + "_negative_tweets.txt", true); 
    	fstreamNeutral = new FileWriter("output/"+candidat + "_neutral_tweets.txt", true);
	    outPositve = new BufferedWriter(fstreamPositive);
	    outNegative = new BufferedWriter(fstreamNegative);
	    outNeutral = new BufferedWriter(fstreamNeutral);

	    outPositve.write("sentiment|tweet");outPositve.newLine();
	    outNegative.write("sentiment|tweet");outNegative.newLine();
	    outNeutral.write("sentiment|tweet");outNeutral.newLine();
	    */

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
	    
	   // String textProcess;
	    int score = 0;
	    	    
		if( "fr".equals(lang) && !"".equals(text)){
			try {
				PreProcessador val = new PreProcessador();
				//text = val.cleanup( text );
				score = DataMining.process( text , lang);
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			if (score > 0) {
			    //outPositve.write( Positive + "|" +  text );outPositve.newLine();
				context.write(new Text(user), Positive);
		        
			}else if (score < 0) {
				//outNegative.write( Negative + "|" + text );outNegative.newLine();
				context.write(new Text(user), Negative);
				
			}else if (score == 0){
				//outNeutral.write( Neutral + "|" + text );outNeutral.newLine();
				context.write(new Text(user), Neutral);
				
			}
		}
	}
}
