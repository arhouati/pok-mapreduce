package pok.mapreduce.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//TODO : refactoring of codes
//TODO : use only ressource of datamining, not copy it
public class Main implements Tool {

    private static final Map<String, List<String>> candidatTags;
    
    // The tags of the different candidates
	static {
		
		@SuppressWarnings("serial")
		Map<String, List<String>> candidatTagsTemp = new HashMap<String, List<String>>() {
			{
				put("Le Pen", Arrays.asList("@MLP_officiel","@FN_officiel", "Le Pen"));
				put("Macron", Arrays.asList("@EmmanuelMacron", "#EmmanuelMacron", "EmmanuelMacron", "@enmarchefr","#Macron", "Macron"));
				put("Fillon", Arrays.asList("@FrancoisFillon","@Fillon2017_fr","@lesRepublicains", "#Fillon", "Fillon"));
				
				put("Mélenchon", Arrays.asList("@JLMelenchon","@jlm_2017"));
				put("Hamon", Arrays.asList("@benoithamon","@AvecHamon2017","@partisocialiste"));
				put("Dupont-Aignan", Arrays.asList("@dupontaignan","@DLF_Officiel"));
				put("Cheminade", Arrays.asList("@JCheminade"));
				put("Arthaud", Arrays.asList("@n_arthaud","@LutteOuvriere"));
				put("Asselineau", Arrays.asList("@UPR_Asselineau","@UPR_Officiel"));
				put("Poutou", Arrays.asList("@PhilippePoutou","@NPA_officiel"));
				put("Lassalle", Arrays.asList("@jeanlassalle"));
			}
		};
		
		candidatTags = Collections.unmodifiableMap(candidatTagsTemp);
        
    }
	
	static private Properties hbasePropertiesFile;

	static private Configuration config;
	
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "/");	
		
        // delete output directory
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("output"), true);

		try {
			
			hbasePropertiesFile = loadPropetyFile("hbase.properties");

		    JobControl jobControl = new JobControl("jobChain"); 

		    System.out.println("# init config :: Standalone HBase without HDFS ");
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.property.clientPort", hbasePropertiesFile.getProperty("hbase.zookeeper.property.clientPort"));
			config.set("mapred.job.tracker", hbasePropertiesFile.getProperty("mapred.job.tracker"));
			config.set("mapred.textoutputformat.separator", ";");
			
			/////////////////////////////////:
			// Define Scan for hbase
			/////////////////////////////////:			
			 
		    // Filter by date  (minDate and maxDate)
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            long minDate = dateFormat.parse("2017/05/10 00:00:00").getTime();
            long maxDate = dateFormat.parse("2017/05/10 23:59:59").getTime();
            			
            byte[] minDateByte = Bytes.toBytes(minDate + "");
            byte[] maxDateByte = Bytes.toBytes(maxDate + "");
            
            // defines filters by date and text
            List<Filter> filters = new ArrayList<Filter>(2);
            byte[] colfam = Bytes.toBytes("Comment");
            byte[] colDate = Bytes.toBytes("Date");
            byte[] mentionMacron = Bytes.toBytes("mention_Macron");
            byte[] mentionFillon = Bytes.toBytes("mention_Fillon");
            byte[] mentionLePen = Bytes.toBytes("mention_LePen");

            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(colfam, colDate , CompareOp.GREATER_OR_EQUAL, minDateByte);  
            filter1.setFilterIfMissing(true); 
            filters.add(filter1);

            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(colfam, colDate, CompareOp.LESS_OR_EQUAL, maxDateByte);          
            filter2.setFilterIfMissing(true);
            filters.add(filter2);
            
            // Filter by keyword on the text
            List<Filter> filtersTextList = new ArrayList<Filter>(2);
            
            String candidate = "Fillon";      

            byte[] colText = Bytes.toBytes("Text");
            SingleColumnValueFilter filterText;
            
            List<String> candidatTagList = candidatTags.get(candidate);
            for( String tag : candidatTagList) {
            	filterText = new SingleColumnValueFilter(colfam, colText, CompareOp.EQUAL, new RegexStringComparator(".*"+tag+".*"));
            	filterText.setFilterIfMissing(true);
            	filtersTextList.add(filterText);
            }
        	
            //FilterList filter3 = new FilterList(FilterList.Operator.MUST_PASS_ONE, filtersTextList);
            //filters.add(filter3);
            
            SingleColumnValueFilter filterMention;
            filterMention = new SingleColumnValueFilter(colfam, mentionLePen, CompareOp.EQUAL, new RegexStringComparator("1"));
            filterMention.setFilterIfMissing(true);
            FilterList filter3 = new FilterList(FilterList.Operator.MUST_PASS_ONE, filterMention);
            filters.add(filter3);         

            // combine all filters
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
       
            // Create a scan
            Scan scan = new Scan();
            
            scan.addFamily(Bytes.toBytes("Comment"));
            scan.addFamily(Bytes.toBytes("User"));
            scan.setCacheBlocks(false);
            scan.setCaching(1000);
            scan.setFilter(filterList);
            
			/////////////////////////////////:
			/////////////////////////////////:
            
            // create Job Map Reduce for Distinct User Sentiment Analysis
		    Job jobDistinctUser = Job.getInstance(config, "Distinct User Sentiment Analysis");
		    jobDistinctUser.setJarByClass(Main.class);
		    
		    // Configure the Map process to use HBase
            TableMapReduceUtil.initTableMapperJob(
                    "pok:election",              // The name of the table
                    scan,                           // The scan to execute against the table
                    MapperDistinctUser.class,                 // The Mapper class
                    Text.class,            			// The Mapper output key class
                    LongWritable.class,             // The Mapper output value class
                    jobDistinctUser ); 
            
		    // Configure the reducer process
            jobDistinctUser.setReducerClass( ReducerDistinctUser.class );
            jobDistinctUser.setCombinerClass( ReducerDistinctUser.class );
		    
            // Setup the output - we'll write to the file system: orientation(positive, negative or neutral)   count
            jobDistinctUser.setOutputKeyClass( Text.class );
            jobDistinctUser.setOutputValueClass( LongWritable.class );
            jobDistinctUser.setOutputFormatClass( TextOutputFormat.class );
            
            // We'll run just one reduce task, but we could run multiple
            jobDistinctUser.setNumReduceTasks( 1 );
            
		    Path outputPath = new Path("output/userdistinct");
		    
		    FileOutputFormat.setOutputPath(jobDistinctUser, outputPath);

		    ControlledJob controlledJobDistinctUser = new ControlledJob(config);
		    controlledJobDistinctUser.setJob(jobDistinctUser);

		    jobControl.addJob(controlledJobDistinctUser);
		    
		    
		    // create Job Map Reduce for Sum Sentiment Analysis
		    config = new Configuration();
		    config.set("key.value.separator.in.input.line", ";");
		    config.set("mapred.textoutputformat.separator", ";");
		    
		    Job jobSumSentimentAnalysis = Job.getInstance(config, "Sum Sentimens Analysis");
		    jobSumSentimentAnalysis.setJarByClass(Main.class);
		    		    
		    jobSumSentimentAnalysis.setMapperClass(MapperSumSentiment.class);
		    jobSumSentimentAnalysis.setReducerClass(ReducerSumSentiment.class);
		    jobSumSentimentAnalysis.setCombinerClass(ReducerSumSentiment.class);
		    
		    jobSumSentimentAnalysis.setOutputKeyClass(Text.class);
		    jobSumSentimentAnalysis.setOutputValueClass(LongWritable.class);
		    
		    jobSumSentimentAnalysis.setInputFormatClass(KeyValueTextInputFormat.class);
		    		
		    FileInputFormat.addInputPath(jobSumSentimentAnalysis, new Path("output/userdistinct/part-r-00000"));
		    FileOutputFormat.setOutputPath(jobSumSentimentAnalysis, new Path("output/final"));
		    		    
		    // We'll run just one reduce task, but we could run multiple
		    jobSumSentimentAnalysis.setNumReduceTasks( 1 );
		    		    
		    ControlledJob controlledJobSumSentimentAnalysis = new ControlledJob(config);
		    controlledJobSumSentimentAnalysis.setJob(jobSumSentimentAnalysis);
		    
		    controlledJobSumSentimentAnalysis.addDependingJob(controlledJobDistinctUser); 

		    // add the job to the job control
		    jobControl.addJob(controlledJobSumSentimentAnalysis); 
		    
		    Thread jobControlThread = new Thread(jobControl);
		    jobControlThread.start();

		    while (!jobControl.allFinished()) {		    	
		        System.out.println("============================================");  
		        System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
		        System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
		        System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
		        System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
		        System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());		        
		        System.out.println("============================================");
		        		        
		        try {
		        	Thread.sleep(5000);
		        } catch (Exception e) {
		        	e.printStackTrace();
		        }

		      } 

		    return ( jobSumSentimentAnalysis.waitForCompletion(true) ? 0 : 1 );

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}

	  } 
	
	public static void main(String[] args) throws ParseException {
	
		int exitCode = 0;
		
		try {
			exitCode = ToolRunner.run(new Main(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		
		System.exit(exitCode);
		
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

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}
}
