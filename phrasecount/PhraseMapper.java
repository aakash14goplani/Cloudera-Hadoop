import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PhraseMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	  	String input = value.toString();        
        input = input.toLowerCase();
        input = input.replaceAll("[\",!.:;?*()']", "");
        input = input.replaceAll("-", "");

        String[] split = input.split(" ");
              
        Map<String, Integer> counts = new HashMap<String,Integer>(split.length*(split.length-1)/2,1.0f);
        int idx0 = 0;
                
        for(int i=0; i<split.length-1; i++)
        {        
            int splitIpos = input.indexOf(split[i],idx0);
            int newPhraseLen = splitIpos - idx0 + split[i].length();
            String phrase = input.substring(idx0, idx0 + newPhraseLen);
            
            for(int j=i+1; j<split.length; j++)
            {          
                newPhraseLen = phrase.length()+split[j].length()+1;
                phrase=input.substring(idx0, idx0 + newPhraseLen);
                Integer count = counts.get(phrase);
                
                if(count==null)
                {
                    counts.put(phrase, 1);
                } 
                else 
                {
                    counts.put(phrase, count+1);
                }
            }
            idx0 = splitIpos+split[i].length()+1;
        }
        
		Map.Entry<String, Integer>[] entries = counts.entrySet().toArray(new Map.Entry[0]);
        Arrays.sort(entries, new Comparator<Map.Entry<String, Integer>>() 
        {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) 
            {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        
        for(Map.Entry<String,Integer> entry:entries)
        {
            int count = entry.getValue();
            String keyans = entry.getKey();
            if(count>1)
            {
				context.write(new Text(keyans), new IntWritable(count));
            }
        }
  }
}