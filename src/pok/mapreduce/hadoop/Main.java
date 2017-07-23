package pok.mapreduce.hadoop;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//TODO : refactoring of codes
public class Main {

	static private Properties hbasePropertiesFile;

	static private Configuration config;
	
	public static void main(String[] args) throws ParseException {
		
		System.setProperty("hadoop.home.dir", "/");	

		try {
			hbasePropertiesFile = loadPropetyFile("hbase.properties");
			
			System.out.println("# init config :: Standalone HBase without HDFS ");
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.property.clientPort", hbasePropertiesFile.getProperty("hbase.zookeeper.property.clientPort"));
			config.set("mapred.job.tracker", hbasePropertiesFile.getProperty("mapred.job.tracker"));
            
		    // Filter by date  (minDate and maxDate)
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            long minDate = dateFormat.parse("2017/04/09 00:00:00").getTime();
            //long maxDate = dateFormat.parse("2017/04/23 23:59:59").getTime();
            long maxDate = dateFormat.parse("2017/04/09 23:59:59").getTime();
            
            byte[] minDateByte = Bytes.toBytes(minDate + "");
            byte[] maxDateByte = Bytes.toBytes(maxDate + "");
            
            // defines filters by date and text
            List<Filter> filters = new ArrayList<Filter>(2);
            byte[] colfam = Bytes.toBytes("Comment");
            byte[] colDate = Bytes.toBytes("Date");
            byte[] colText = Bytes.toBytes("Text");

            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(colfam, colDate , CompareOp.GREATER_OR_EQUAL, minDateByte);  
            filter1.setFilterIfMissing(true); 
            filters.add(filter1);

            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(colfam, colText, CompareOp.LESS_OR_EQUAL, maxDateByte);          
            filter2.setFilterIfMissing(true);
            filters.add(filter2);
            
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
       
            // Create a scan
            Scan scan = new Scan();
            
            scan.addFamily(Bytes.toBytes("Article"));
            scan.setCacheBlocks(false);
            scan.setCaching(1000);
            //scan.setFilter(filterList);
           
            // create Job Map Reduce
		    Job job = Job.getInstance(config, "Text Sentiment");
		    job.setJarByClass(Main.class);
		    
            // Configure the Map process to use HBase
            TableMapReduceUtil.initTableMapperJob(
                    "poc:election-fr",              // The name of the table
                    scan,                           // The scan to execute against the table
                    MyMapper.class,                 // The Mapper class
                    Text.class,            			// The Mapper output key class
                    LongWritable.class,             // The Mapper output value class
                    job ); 
            
		    // Configure the reducer process
            job.setReducerClass( MyReducer.class );
            job.setCombinerClass( MyReducer.class );
		    
            // Setup the output - we'll write to the file system: orientation(positive, negative or neutral)   count
            job.setOutputKeyClass( Text.class );
            job.setOutputValueClass( LongWritable.class );
            job.setOutputFormatClass( TextOutputFormat.class );
            
            // We'll run just one reduce task, but we could run multiple
            job.setNumReduceTasks( 1 );
            			
            // delete output directory
            FileSystem fs = FileSystem.get(new Configuration());
            fs.delete(new Path("output"), true);
            
		    FileOutputFormat.setOutputPath(job, new Path( "output" ));
		    		
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static Properties loadPropetyFile( String file) throws IOException{
		
		Properties propetiesFile = new Properties();
		InputStream inStream = Main.class.getClassLoader().getResourceAsStream( file );
		if( inStream != null){
			propetiesFile.load(inStream);
		}else{
			throw new FileNotFoundException("propety file : " + file  + " not found in the classpath");
		};
		
		return propetiesFile;
		
	}
}
