package tfidf1;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public static Text tf;
	public static Text idf;
	public static String docPath = null;
	public String vocabPath = null;
	public long filesCount;
	
	public void setup(Context context) throws IOException{
//		String docP = jobConf.get("InputPath");
//		String vocabP = jobConf.get("InputPathVocab");
//		
		
		Configuration conf = context.getConfiguration();
		
		docPath = conf.get("InputPath").toString();
		//vocabPath = conf.get("InputPathVocab").toString();
		System.err.println("Doc path "+docPath);
		//System.err.println("Vocab path"+vocabPath);
		
		Path path = new Path(docPath);
		FileSystem fs = path.getFileSystem(conf);
		FileStatus[] fileStatus = fs.listStatus(path);
		filesCount = fileStatus.length;		
				
		
		
		
		System.err.println("The total number of docs "+filesCount);		
		
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
			String[] values = value.toString().split(":");
			//key.set(values[0]);
			
			String[] tfidf = values[1].split("-");
			DecimalFormat df = new DecimalFormat("###.#####");
			double tf = Double.parseDouble(tfidf[0]);
			double idf_cal =(double) filesCount/(Double.parseDouble(tfidf[1]));
			double idf = (double) (Math.log10(idf_cal));
			
			String tfidf_final = df.format((tf*idf)); 
			
			
			
			
		
		
		context.write(new Text(values[0].toString()),new Text(tfidf_final));
		
	}
	
	
	
	
	
	

}
