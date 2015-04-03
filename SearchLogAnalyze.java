package ac.ucas.SearchLogAnalyze;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.lang.Integer;
import java.lang.Long;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SearchLogAnalyze {
	//定义一个临时文件夹，存放作业count的输出
	static Path tempDir = new Path("wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
	
	public static class SearchLogAnalyzeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
	    /*检查读入的条目是否符合指定条件，如果是则map，否则跳过不处理*/
	    public static  boolean meetCondictions(Context context,Date date,String ip){
	    	//获取ip的最后一部分
			int ipEnd = Integer.parseInt(ip.substring(ip.lastIndexOf(".") + 1));
			//获取指定的条件
			String startDate = context.getConfiguration().get("StartDate");
			String endDate = context.getConfiguration().get("EndDate");
			String ipPrefix = context.getConfiguration().get("IpPrefix");
			String sStartIp = context.getConfiguration().get("StartIp");
			String sEndIp = context.getConfiguration().get("EndIp");
			int startIp = 0;
			int endIp = 127;
			if(sStartIp != null)
				startIp = new Integer(sStartIp).intValue();
			if(sEndIp != null)
				endIp = new Integer(sEndIp).intValue();

			try {
				if(
						( startDate == null || date.after(new SimpleDateFormat("yyyy-MM-dd").parse(startDate) ) ) &&
						( endDate == null || date.before(new SimpleDateFormat("yyyy-MM-dd").parse(endDate) ) ) &&
						( ipPrefix == null || ipPrefix.equals(ip.substring(0, ip.lastIndexOf(".") ) ) ) &&
						( startIp <= ipEnd ) &&
						( endIp >= ipEnd )
						)
						return true;
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return false;
		}
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()){
	    	  Date date = null;
	    	  try {
	    		  date = new SimpleDateFormat("yyyy-MM-dd").parse(itr.nextToken());
			} catch (ParseException e) {
				e.printStackTrace();
			}
	    	itr.nextToken();					//时间
	    	String query = itr.nextToken();		//检索词
	    	String ip = itr.nextToken();		//IP
	    	
	    	//如果符合条件则进行map处理，否则忽略
	    	if(meetCondictions(context,date, ip)){
					word.set(query);
					context.write(word, one);
			}
	      }
	    }
	}
	
	public static class SearchLogAnalyzeCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

		private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	}
	
	public static class SearchLogAnalyzeSortMapper extends Mapper<Text, IntWritable, IntWritable, Text>{
	    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
					context.write(value, key);
	    }
	}
	
	public static class SearchLogAnalyzeSortReducer extends Reducer<IntWritable,Text,Text,IntWritable>{

	    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	    	int iTopK = new Integer(context.getConfiguration().get("TopK")).intValue();
	    	for (Text val : values) {
	    		if(iTopK > 0){
	    			context.write(val,key);
	    			iTopK--;
	    			}
	    		else{
	    			context.getConfiguration().set("TopK", new Integer(0).toString());
	    			return ;
	    			}
	    		}
	    	context.getConfiguration().set("TopK", new Integer(iTopK).toString());
	    	}
	    }
	
	//hadoop默认按照key升序排序，要实现降序排序需要重新定义排序类
	private static class IntWritableDecreasingComparator extends IntWritable.Comparator {  
        public int compare(@SuppressWarnings("rawtypes") WritableComparable a, @SuppressWarnings("rawtypes") WritableComparable b) {  
          return -super.compare(a, b);  
        }  
          
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
            return -super.compare(b1, s1, l1, b2, s2, l2);  
        }  
    }
	
	public static void main(String[] args) throws Exception {
		if(args.length < 2){
			if(args.length == 0){
				System.err.println("<in> <out> directories needed,type -h to see help information");
				System.exit(1);
			}
			if(args[0].equals("-h")){
				System.out.println("Usage: SearchLog.jar <int> <out> [-parameters]");
				System.out.println("-NumOfRecord	long		set the number of searchlog records(default:10)");
				System.out.println("-LogFileName	String		set the filename of logfile(default:<in>/SearchLog.txt)");
				System.out.println("-TopK		int		Return the top K results(default:50)");
				System.out.println("-StartDate	yyyy-MM-dd	set the StartDate of filter(default:2000-01-01)");
				System.out.println("-EndDate	yyyy-MM-dd	set the EndDate of filter(default:2014-12-30)");
				System.out.println("-IpPrefix	x.x.x		set the IpPrefix of filter(default:null)");
				System.out.println("-StartIp	int		set the StartIp of filter(default:0)");
				System.out.println("-EndIp		int		set the EndIp of filter(default:127)");
				System.exit(1);
			}
        }
		
		Log log = new Log();
		log.setPath(args[0] + "/SearchLog.txt");
		Configuration conf = new Configuration();
		conf.set("TopK", "50");
		
		for(int i = 2; i < args.length - 1;i += 2){
			switch(args[i]){
			case ("-NumOfRecord"):	log.setNumOfRecord(Long.parseLong(args[i + 1]));break;
			case ("-LogFileName"):	log.setPath(args[0] + "/" + args[i + 1]);break;
			case ("-TopK"):			conf.set("TopK", args[i + 1]);break;
			case ("-StartDate"):	conf.set("StartDate", args[i + 1]);break;
			case ("-EndDate"):		conf.set("EndDate", args[i + 1]);break;
			case ("-IpPrefix"):		conf.set("IpPrefix", args[i + 1]);break;
			case ("-StartIp"):		conf.set("StartIp", args[i + 1]);break;
			case ("-EndIp"):		conf.set("EndIp", args[i + 1]);break;
			default:				System.out.println("Unknow parameters:" + args[i] + " ignored");break;
			}
		}
		
		log.logWriter();
		//log.logReader();
		try {
			Job countJob = Job.getInstance(conf, "SearchLogAlalyze-Count");	//count作业	
			FileInputFormat.addInputPath(countJob, new Path(args[0]));
			FileOutputFormat.setOutputPath(countJob, tempDir);				//临时文件夹作为输出目录
			countJob.setJarByClass(SearchLogAnalyze.class);
			countJob.setMapperClass(SearchLogAnalyzeCountMapper.class);		//指定mapper类
			countJob.setCombinerClass(SearchLogAnalyzeCountReducer.class);	//指定reducer类
			countJob.setReducerClass(SearchLogAnalyzeCountReducer.class);
			countJob.setOutputKeyClass(Text.class);
			countJob.setOutputValueClass(IntWritable.class);
			countJob.setOutputFormatClass(SequenceFileOutputFormat.class);	//输出格式,作为下一个job的输入
			
			if(countJob.waitForCompletion(true)){
				Job sortJob = Job.getInstance(conf, "SearchLogAlalyze-Sort");//sort作业
				FileInputFormat.addInputPath(sortJob, tempDir);  			 //临时文件作为输入路径
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);	 //输入格式
				FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));  //输出路径
				sortJob.setJarByClass(SearchLogAnalyze.class);
				sortJob.setMapperClass(SearchLogAnalyzeSortMapper.class);
				sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
				sortJob.setNumReduceTasks(1);
				sortJob.setMapOutputKeyClass(IntWritable.class);
				sortJob.setMapOutputValueClass(Text.class);
				sortJob.setReducerClass(SearchLogAnalyzeSortReducer.class);	 //指定reducer类
				sortJob.setOutputKeyClass(Text.class);  
				sortJob.setOutputValueClass(IntWritable.class);
				System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
				}
			} catch (Exception e){
				e.printStackTrace();
				}finally{
					FileSystem.get(conf).deleteOnExit(tempDir);
					}
	}
}