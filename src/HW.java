import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class HW{

 public static class Map_following extends Mapper<LongWritable, Text, IntWritable, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens())  {
	    String token1 = tokenizer.nextToken();
	    String token2 = tokenizer.nextToken();
            context.write(new IntWritable(Integer.parseInt(token1)), new Text(token2));
        }
    }
 } 
        
 public static class Reduce_following extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    public void reduce(IntWritable key,Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
	String str = new String();
	str = "";
	for(Text val : values){
	    str = str + val.toString() + " " ;
	}
	context.write(key, new Text(str));
    }
 }



 public static class Map_follower extends Mapper<LongWritable, Text, IntWritable, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	String str_following = "following";
	String str_follower  = "follower";
	StringTokenizer tokenizer = new StringTokenizer(line);
	String token1 = tokenizer.nextToken();
	while(tokenizer.hasMoreTokens()){
	    String token2 = tokenizer.nextToken();
	    context.write(new IntWritable(Integer.parseInt(token1)),new Text(str_following + token2));//add the "following" tag before the number
	    context.write(new IntWritable(Integer.parseInt(token2)),new Text(str_follower + token1)); //add the "follower"  tag before the number
	}
    }
 }

 public static class Reduce_follower extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	String str = new String();
	String followinglist = "following_list-->";
	String followerlist = "follower_list -->";
	for(Text val : values){
	    String temp = val.toString();
	    if(temp.charAt(6)=='i'){       //check the value has "following" tag + number
    		temp = temp.substring(9);  // remove "following" tag
		followinglist = followinglist + temp + " "; //add the number into the following list
	    }
	    else if(temp.charAt(6)=='e'){  //check the value has "follower" tag + number
		temp = temp.substring(8);  //remove  "follower" tag
		followerlist = followerlist + temp + " "; //add the number into the follower list
	    }
	}
	if(followinglist=="following_list-->")
	    followinglist = followinglist + "None";
	if(followerlist=="follower_list -->")
	    followerlist = followerlist + "None";
	context.write(key, new Text(followinglist));
	context.write(key, new Text(followerlist));
    }
 }



 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = new Job(conf, "following");

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Map_following.class);
    job.setReducerClass(Reduce_following.class);
    job.setJarByClass(WordCount.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);



    Configuration  conf2 = new Configuration();

    Job job2 = new Job(conf2, "follower");

    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);

    job2.setMapperClass(Map_follower.class);
    job2.setReducerClass(Reduce_follower.class);
    job2.setJarByClass(WordCount.class);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    job2.waitForCompletion(true);

 }

}
