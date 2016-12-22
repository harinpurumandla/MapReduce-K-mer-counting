package org.myorg;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmer {

public static class Mapper1
extends Mapper<Object, Text, Text, IntWritable>{
public static String leftout1="";
public static String leftout2="";
public static boolean flag=false;
private final static IntWritable one = new IntWritable(1);

public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
		String line = value.toString();
		if(flag)
		{
		leftout1 +=line;
		int i=0;
		for(i=0;i<=leftout1.length()-10;i++)
		{
			context.write(new Text(leftout1.substring(i, i+10)), one);
		}
		leftout1=leftout1.substring(i,leftout1.length());
		leftout2 +=line;
		i=0;
		for(i=0;i<=leftout2.length()-20;i++)
		{
			context.write(new Text(leftout2.substring(i, i+20)), one);
		}
		leftout2=leftout2.substring(i,leftout2.length());
		}
		else
			flag=true;
	}
}
public static class Reducer1
extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	int sum = 0;
	for (IntWritable val : values) {
		sum += val.get();
	} 
	result.set(sum); 
	context.write(key, result);
	}
}
public static class Mapper2
extends Mapper<Object, Text, LongWritable, Text>{

public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
		String[] parts=value.toString().split("\t");
		context.write(new LongWritable(Integer.parseInt(parts[1])), new Text(parts[0]));
	}
}
public static class Reducer2
extends Reducer<LongWritable,Text,Text,Text> {
public static int count=1;
public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	for (Text val : values) {
		if(val.toString().length()==10 && count<=10)
		context.write(val,new Text(""+key.get()));
		count++;
	} 
	}
}
public static class Reducer3
extends Reducer<LongWritable,Text,Text,Text> {
public static int count=1;
public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	for (Text val : values) {
		if(val.toString().length()==20 && count<=10)
		{
		context.write(val,new Text(""+key.get()));
		count++;
		} 
	}
	}
}
public static void main(String[] otherArgs) throws Exception { 
	Configuration conf = new Configuration();

	if (otherArgs.length != 2) { 
		System.err.println("Usage: kmer<in> <out>"); System.exit(2);
	}

	Job job = new Job(conf, "kmer10"); 
	job.setJarByClass(Kmer.class); 
	job.setMapperClass(Mapper1.class); 
	job.setReducerClass(Reducer1.class); 
	job.setOutputKeyClass(Text.class); 
	job.setOutputValueClass(IntWritable.class);

	FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/user/vcslstudent/"+otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/user/vcslstudent/"+otherArgs[1]+"/temp")); 
	boolean success=job.waitForCompletion(true);
	if(success)
	{
	    Job job2 = Job.getInstance(conf, "JOB_2");
	    job2.setJarByClass(Kmer.class); 
	    job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	    job2.setMapperClass(Mapper2.class);
	    job2.setReducerClass(Reducer2.class);
	    job2.setMapOutputKeyClass(LongWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(Text.class); 
		job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path("hdfs://master:9000/user/vcslstudent/"+otherArgs[1]+"/temp"));
	    FileOutputFormat.setOutputPath(job2, new Path("hdfs://master:9000/user/vcslstudent/"+otherArgs[1]+"/kmer10"));
	    success=job2.waitForCompletion(true);
	}
	if(success)
	{
	    Job job3 = Job.getInstance(conf, "JOB_3");
	    job3.setJarByClass(Kmer.class); 
	    job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	    job3.setMapperClass(Mapper2.class);
	    job3.setReducerClass(Reducer3.class);
	    job3.setMapOutputKeyClass(LongWritable.class);
	    job3.setMapOutputValueClass(Text.class);
	    job3.setOutputKeyClass(Text.class); 
		job3.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job3, new Path("hdfs://master:9000/user/vcslstudent/"+otherArgs[1]+"/temp"));
	    FileOutputFormat.setOutputPath(job3, new Path("hdfs://master:9000/user/vcslstudent/"+otherArgs[1]+"/kmer20"));
	    success=job3.waitForCompletion(true);
	}
	System.exit(success? 0 : 1);
	}
}
