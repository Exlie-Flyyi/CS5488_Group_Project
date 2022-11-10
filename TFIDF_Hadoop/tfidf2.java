import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


/**
 * Map: get	[doc: word_n]
 * Reduce: get [word_doc, n/N] (tf)
**/
public class tfidf2{
	public static class tfidfMapper2
			extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context
						) throws IOException, InterruptedException {
			String[] word_doc = key.toString().split('_');
			context.write(new Text(word_doc[0]), new Text(word_doc[0]+"_"+value.toString()));
	    }
	}
	public static class tfidfReducer2 
			extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context
						) throws IOException, InterruptedException {
			
			int wordsInDoc = 0;
			Map<String, Integer> tempCounter = new HashMap<String, Integer>();
			for(Text val : values){
				String[] wordcount = val.toString().split(",");
				tempCounter.put(wordcount[0], Integer.valueOf(wordcount[1]));
				wordsInDoc += Integer.parseInt(wordcount[1]);
			}
			for (String wordKey : tempCounter.keySet()){
				context.write(new Text(wordKey + "_" + key.toString()), new Text(tempCounter.get(wordKey) + "/" + wordsInDoc)); 
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf2 = new Configuration();
		conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Job job2 = Job.getInstance(conf, "getTF");
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setJarByClass(tfidf2.class);
		job2.setMapperClass(tfidfMapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(tfidfReducer2.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}