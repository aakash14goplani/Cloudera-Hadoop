import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PhraseReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		int phraseCount = 0;

		while (values.iterator().hasNext())
		{
			values.iterator().next(); 
			phraseCount++;
		}

		context.write(key, new IntWritable(phraseCount));
	}
} 