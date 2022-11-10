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


/**
 * Map: get	[word: doc_n/N]
 * Reduce: get [word_doc, n/N*log(D/d)] (tf-idf)
**/
public class tfidf3{
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
		Configuration conf3 = new Configuration();
		conf3.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		
		int numDocs = 0;
		File file = new File("tfidf/input");
		File[] fs = file.listFiles();
		for(File f:fs){
			if(!f.isDirectory()){
				LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(f));
	        	lineNumberReader.skip(Long.MAX_VALUE);
	        	int lineNumber = lineNumberReader.getLineNumber();
				numDocs += lineNumber + 1;}
		}
		conf3.setInt("numDocs", numDocs);
		//conf3.setInt("numDocs", Integer.parseInt(args[3]));
		Job job3 = Job.getInstance(conf3, "getTF-IDF");
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setJarByClass(tfidf3.class);
		job3.setMapperClass(tfidfMapper3.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setReducerClass(tfidfReducer3.class);
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
}