package tfidf1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IdfReducerTask extends Reducer<Text ,Text,Text,Text> {
	
	String[] val;
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		long sum = 0;
		long term_in_doc_count = 0;
		Map<String,String> doc_id=new HashMap<String,String>();
		Iterator<Text> itr = values.iterator();
		while(itr.hasNext()){
			val = itr.next().toString().split("-");
			sum=sum + Integer.parseInt(val[2]);		
			doc_id.put(key.toString(), val[0].toString());
			
		}
		term_in_doc_count = sum;
		
		for(Entry<String, String> doc_terms: doc_id.entrySet()){
		  context.write(new Text(doc_terms.getKey()+"\t"+doc_terms.getValue()), new Text(val[1].toString()+"-"+sum));
		  
		  System.err.println("The doc-term is " +doc_terms.getKey()+"-"+doc_terms.getValue());
		}	
		
		//context.write(new Text(doc_terms.getKey()), new Text(val[1].toString()+"-"+sum));

}


}
