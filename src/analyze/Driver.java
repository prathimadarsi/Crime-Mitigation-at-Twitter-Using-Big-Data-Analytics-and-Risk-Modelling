package analyze;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SentimentAnalysis Preprocessing");
		job.setJarByClass(Driver.class);
		//System.out.println("Working here before mapper");
		job.setMapperClass(TwitterMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		
		try
		{
			System.out.println("Working here before cache");
			job.addCacheFile(new Path(args[2]).toUri());
			
			//job.addCacheFile(new URI("hdfs://localhost:50070/cache"));
			
		}
		catch(Exception e)
			{
				System.out.println("file not added");
				System.exit(1);
			}
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	//	System.out.println("Working here");
		
		System.exit(job.waitForCompletion(true)?0:1);
//		System.out.println("Working here exit");
		
	}
		
}