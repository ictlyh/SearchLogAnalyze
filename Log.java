package ac.ucas.SearchLogAnalyze;

import java.lang.Math;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class Log {
	private String path = "";		//日志文件路径
	private long numOfRecord = 10;	//日志条目数目
	
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public long getNumOfRecord() {
		return numOfRecord;
	}
	public void setNumOfRecord(long numOfRecord) {
		this.numOfRecord = numOfRecord;
	}
	/*生成随机字符，包括数字和大小写字母*/
	private char randomChar(){
		int tmp = 0;
		do{
			tmp = (int)(Math.random() * 122);
		}while( !( (48 <= tmp && tmp <= 57) || (65 <= tmp && tmp <= 90) || (97 <= tmp && tmp <= 122) ) );
		return (char)tmp;
	}
	
	/*随机生成IP地址*/
	private String randomIp(){
		String ip = null;
		ip = new Integer((int)(Math.random() * 128)).toString() + "." +
				new Integer((int)(Math.random() * 128)).toString() + "." +
				new Integer((int)(Math.random() * 128)).toString() + "." +
				new Integer((int)(Math.random() * 128)).toString();
		return ip;
	}
	
	/*随机生成一个检索记录*/
	private String generateRecord(){
		String result = "";
		int year = 2000 + (int)(Math.random() * 14);	//起始年份2000年
		int month = 1 + (int)(Math.random() * 11);
		int day = 1 + (int)(Math.random() * 29);		//没有考虑个别月份有31号的情况
		int hour = (int)(Math.random() * 23);
		int minute = (int)(Math.random() * 59);
		int second = (int)(Math.random() * 59);
		
		String sYear = new Integer(year).toString();
		String sMonth = (month < 10 ? "-0" : "-") + new Integer(month).toString();
		String sDay = (day < 10 ? "-0" : "-") + new Integer(day).toString();
		String sHour = (hour < 10 ? " 0" : " ") + new Integer(hour).toString();
		String sMinute = (minute < 10 ? ":0" : ":") + new Integer(minute).toString();
		String sSecond = (second < 10 ? ":0" : ":") + new Integer(second).toString();
		
		String query = "";
		int lengthOfQuery = (int)(Math.random() * 4 + 1);	//随机生成搜索词长度，1-5
		for(int i = 0; i < lengthOfQuery; i++)
		{
			char tmp = randomChar();
			query += tmp;
		}
		result = sYear + sMonth + sDay + sHour + sMinute + sSecond + " " + query + " " + randomIp();
		return result;
	}
	
	/*将随机生成的检索记录写入日志文件*/
	public void logWriter() throws IOException{
		FileSystem fs = FileSystem.get( new Configuration() );
		FSDataOutputStream fos = fs.create(new Path(getPath()),true);
		Writer out = new OutputStreamWriter(fos,"utf-8");
		for(long i = 0; i < numOfRecord; i++){
			out.write(generateRecord() + "\n");
		}
		out.close();
		fos.close();
	}
	
	/*读取日志文件到stdout*/
	public void logReader() throws IOException{
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream fis = fs.open(new Path(getPath()));
		InputStreamReader isr = new InputStreamReader(fis,"utf-8");
		BufferedReader br = new BufferedReader(isr);
		String line = "";
		while((line = br.readLine()) != null){
			System.out.println(line);
		}
		br.close();
		isr.close();
		fis.close();
	}
}