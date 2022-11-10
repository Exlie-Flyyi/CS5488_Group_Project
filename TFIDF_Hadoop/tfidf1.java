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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
public class tfidf1{
/**
 * [word_doc: n]
**/
	public static class tfidfMapper1
			extends Mapper<Text, Text, Text, IntWritable> {
		private final Text wordKey = new Text();
		private final static IntWritable one = new IntWritable(1);
		public void map(Text key, Text value, Context context
						) throws IOException, InterruptedException {
	        StringTokenizer itr = new StringTokenizer(value.toString());
	        while (itr.hasMoreTokens()) {
	        	String word = itr.nextToken();
	        	wordKey.set(word + "_" + key);
	        	context.write(wordKey, one);
	        }
	    }
	}

	public static class tfidfReducer1 
			extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += 1;
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Job job = Job.getInstance(conf, "Word Frequence");
		job.setJarByClass(tfidf1.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(tfidfMapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(tfidfReducer1.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}