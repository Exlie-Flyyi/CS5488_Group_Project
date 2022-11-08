import java.net.URI;
import java.util.Arrays;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class tfidf {

/**
 * number of occureneces of word i in posting j tf_ij
 * total number of postings N 
 * */
	public static class tfidfMapper1
			extends Mapper<Text, Text, Text, IntWritable> {
		private final Text wordKey = new Text();
		public final static Text counter = new Text("count");
		private final static IntWritable one = new IntWritable(1);

		public void map(Text key, Text value, Context context
						) throws IOException, InterruptedException {
	        StringTokenizer itr = new StringTokenizer(value.toString());
	        while (itr.hasMoreTokens()) {
	        	String word = itr.nextToken();
	        	wordKey.set(word + "_" + key);
	        	// tf_ij
	        	context.write(wordKey, one);
	        	// output the length of posting
	        	//context.write(key,one);
	        }
	        // number of postings
	        //context.write(counter,one);
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

	public static class tfidfMapper2
			extends Mapper<Text, IntWritable, Text, Text> {
		public void map(Text key, Text value, Context context
						) throws IOException, InterruptedException {
	    	String[] word_doc = key.toString().split("_");
	    	context.write(new Text(word_doc[1]),new Text(word_doc[0]+","+value.toString()));
	    }
	}

	public static class tfidfReducer2
			extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
	    Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
	    Job job = Job.getInstance(conf, "Word Frequence In Document");
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setJarByClass(tfidf.class);
	    job.setMapperClass(tfidfMapper1.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(tfidfReducer1.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);

	    Configuration conf2 = new Configuration();
		conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
	    Job job2 = Job.getInstance(conf2, "getTF");
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setJarByClass(tfidf.class);
	    job2.setMapperClass(tfidfMapper2.class);
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setReducerClass(tfidfReducer2.class);
	    job2.waitForCompletion(true);
	    FileInputFormat.addInputPath(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    job2.waitForCompletion(true);

	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}