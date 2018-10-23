import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriends {
		
	
	public static class map extends Mapper <LongWritable,Text,Text,Text>{
		
		private Text word = new Text() ;
		
		public void map (LongWritable key , Text value , Context context) throws IOException, InterruptedException {
			String [] line = value.toString().split("\t");
			if (line.length == 2) {
			String userid = line[0];
			List<String> values = Arrays.asList(line[1].split(",")) ;
			for (String friends : values){
				int useridval = Integer.parseInt(userid);
				int friendsval = Integer.parseInt(friends);
				
				if ( useridval < friendsval)
					word.set(useridval + "," + friendsval);
				else
					word.set(friendsval + "," + useridval);
					context.write(word, new Text(line[1]));
			}
			
		}
			
		}
		
	}
	
	
	public static class reduce extends Reducer <Text,Text,Text,Text>{
		private Text result = new Text() ;
		
		public void reduce (Text key , Iterable<Text> values , Context context) throws IOException, InterruptedException {
			HashMap <String, Integer> map = new HashMap<String,Integer>() ;
			StringBuilder sb = new StringBuilder() ;
			for ( Text friends : values){
				List<String> temp = Arrays.asList(friends.toString().split(",")) ;
				for ( String friend : temp) {
					if (map.containsKey(friend)){
						sb.append(friend + ",") ;
					}
					else
						map.put(friend, 1);
				}
			}
			
			result.set(new Text(sb.substring(0,sb.length()-1)));
			context.write(key, result);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: Mutual Friend <inputfile hdfs path> <output file hdfs path>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(map.class);
		job.setReducerClass(reduce.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
