import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.io.FileReader;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import java.text.DecimalFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.LineNumberReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;



public class tfidf{
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


/**
 * Map: get	[doc: word_n]
 * Reduce: get [word_doc, n/N] (tf)
**/
	public static class tfidfMapper2
			extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context
						) throws IOException, InterruptedException {
	    	//StringTokenizer itr = new StringTokenizer(value.toString());
	    	String[] word_doc = key.toString().split("_");
	    	context.write(new Text(word_doc[1]),new Text(word_doc[0]+","+value.toString()));
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

/**
 * Map: get	[word: doc_n/N]
 * Reduce: get [word_doc, n/N*log(D/d)] (tf-idf)
**/
	public static class tfidfMapper3
			extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context
						) throws IOException, InterruptedException {
	    	String[] word_doc = key.toString().split("_");
	    	context.write(new Text(word_doc[0]),new Text(word_doc[1]+","+value.toString()));
	    }
	}

	public static class tfidfReducer3 
			extends Reducer<Text, Text, Text, Text> {
		private static final DecimalFormat DF = new DecimalFormat("###.########");
		private Text wordAtDocument = new Text();
		private Text tfidfCounts = new Text();


		public void reduce(Text key, Iterable<Text> values, Context context
						) throws IOException, InterruptedException {

			int numDocs = context.getConfiguration().getInt("numDocs", 0);

			//context.write(test, new Text(String.valueOf(numDocs)));
			//int numDocs = 3;
			int keyAppears = 0;
			Map<String, String> tempFrequencies = new HashMap<String, String>();

			for (Text val : values) {
				String[] temp = val.toString().split(",");
				keyAppears++;
				tempFrequencies.put(temp[0], temp[1]);
			}

			for (String document : tempFrequencies.keySet()) {
				String[] temp1 = tempFrequencies.get(document).split("/");

				double tf = Double.valueOf(Double.valueOf(temp1[0]) / Double.valueOf(temp1[1]));
				double idf = Math.log10((double) numDocs / (double) (keyAppears));

				double tfIdf = tf * idf;
				this.wordAtDocument.set(key + "@" + document);
				this.tfidfCounts.set( DF.format(tfIdf));
				context.write(this.wordAtDocument, this.tfidfCounts);
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Job job = Job.getInstance(conf, "Word Frequence");
		job.setJarByClass(tfidf.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(tfidfMapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(tfidfReducer1.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		int numDocs = 0;
		File file = new File(args[0]);
		File[] fs = file.listFiles();
		for(File f:fs){
			if(!f.isDirectory()){
				LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(f));
	        	lineNumberReader.skip(Long.MAX_VALUE);
	        	int lineNumber = lineNumberReader.getLineNumber();
				numDocs += lineNumber + 1;}
		}

		Configuration conf2 = new Configuration();
		conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Job job2 = Job.getInstance(conf, "getTF");
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setJarByClass(tfidf.class);
		job2.setMapperClass(tfidfMapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(tfidfReducer2.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);

		Configuration conf3 = new Configuration();
		conf3.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		//conf3.setInt("numDocs", Integer.parseInt(args[4]));
		conf3.setInt("numDocs", numDocs);
		Job job3 = Job.getInstance(conf3, "getTF-IDF");
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setJarByClass(tfidf.class);
		job3.setMapperClass(tfidfMapper3.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setReducerClass(tfidfReducer3.class);
		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
}
