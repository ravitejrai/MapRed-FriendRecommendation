import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin {
	
	public static class FriendMapper extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		LongWritable userId=new LongWritable();
		
		public void map(LongWritable key, Text value,Context context ) throws IOException, InterruptedException 
		{
			String line[]=value.toString().split("\t");
			userId.set(Long.parseLong(line[0]));
			if(line.length==2)
			{
				String friends=line[1];
				String outvalue="U:"+friends.toString();
				context.write(userId, new Text(outvalue));
			}
		}
	}
	
	public static class DetailsMapper extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		LongWritable outkey=new LongWritable();
		Text outvalue=new Text();
		public void map(LongWritable key, Text value,Context context ) throws IOException, InterruptedException
		{
			String input[]=value.toString().split(",");
			if(input.length==10)
			{
				outkey.set(Long.parseLong(input[0]));
				String[] cal=input[9].toString().split("/");
				Date currDate=new Date();
				int currMonth=currDate.getMonth()+1;
				int currYear=currDate.getYear()+1900;
				int currDay=currDate.getDate();
				int result=currYear-Integer.parseInt(cal[2]);
				if(Integer.parseInt(cal[0])>currMonth)
				{
					result--;
				}
				else if(Integer.parseInt(cal[0])==currMonth){
					if(Integer.parseInt(cal[1])>currDay)
						result--;					
				}
				String data=input[1]+","+new Integer(result).toString()+","+input[3]+","+input[4]+","+input[5];
				outvalue.set("R:"+data);
				context.write(outkey, outvalue);				
			}
		}
	}
	
	public static class JoinReducer extends Reducer<LongWritable, Text, Text, Text>
	{
		static HashMap<String, String> userData;
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		
		public void setup(Context context) throws IOException{
			Configuration config=context.getConfiguration();
			userData = new HashMap<String, String>();
			String userDataPath =config.get("userdata");
			FileSystem fs = FileSystem.get(config);
			Path path = new Path("hdfs://localhost:9000"+userDataPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 10) {
					String data = arr[1] + ":" + arr[3]+":"+arr[9];
					userData.put(arr[0].trim(), data);
				}
				line = br.readLine();
			}
		}
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			listA.clear();
			listB.clear();
			for(Text val: values)
			{
				if(val.toString().charAt(0)=='U')
					listA.add(new Text(val.toString().substring(2)));
				else if(val.toString().charAt(0)=='R')
					listB.add(new Text(val.toString().substring(2)));
			}
			String[] details=null;
			Text C = new Text();
			if(!listA.isEmpty() && !listB.isEmpty())
			{
				for(Text A:listA)
				{
					float age=0;
					float minage=Integer.MAX_VALUE;
					String[] friend=A.toString().split(",");
					for(int i=0;i<friend.length;i++)
					{
						if(userData.containsKey(friend[i]))
						{
							String[] ageCal=userData.get(friend[i]).split(":");
							Date curr=new Date();
							int currMonth=curr.getMonth()+1;
							int currYear=curr.getYear()+1900;
							String[] cal=ageCal[2].toString().split("/");
							int result=currYear-Integer.parseInt(cal[2]);
							if(Integer.parseInt(cal[0])>currMonth)
								result--;
							else if(Integer.parseInt(cal[0])==currMonth)
							{
								int currDay=curr.getDate();
								if(Integer.parseInt(cal[1])>currDay)
									result--;
							}
							//System.out.println(result);
							age=result;
						}
						if(age < minage)
							minage=age;						
					}
					
					String subdetails="";
					StringBuilder res=new StringBuilder();
					for(Text B:listB)
					{
						details=B.toString().split(",");
						subdetails=B.toString()+","+new Text(new FloatWritable((float) minage).toString());
						res.append(B.toString());
						res.append(",");
						res.append(new Text(new FloatWritable((float) minage).toString()));
					}
					C.set(res.toString());
				}
			}
			context.write(new Text(key.toString()), C);
		}
	}

	public static class MinAgeMapper extends Mapper<LongWritable,Text,IntWritable, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] m=value.toString().split("\t");
			if(m.length==2)
			{
				
				String line[]=m[1].split(",");
				String passval = m[0].toString() ;
				context.write(new IntWritable(Integer.parseInt(m[0])), new Text(m[1].toString()));
			}
		}
	}
	

	
	public static class MinAgeReducer extends Reducer<IntWritable, Text, Text, Text> 
	{
		TreeMap<String,String> output=new TreeMap<String, String>();        
	
		         
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for(Text t:values)
			{
				if(output.size()<10)
				{
					output.put(key.toString(), t.toString());
					context.write(new Text(t.toString().split(",")[0]), new Text(t));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{

		Path outputDirIntermediate1 = new Path(args[3] + "_int1");
		Path outputDirIntermediate2 = new Path(args[4]);
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		conf.set("userdata",otherArgs[0]);
		// get all args
		if (otherArgs.length != 5)
		{
			System.err.println("Usage: JoinExample <inmemory input> <input > <input> <intermediate output> <output>");
			System.exit(2);
		}
		
		Job job = new Job (conf, "join1");
		job.setJarByClass(ReduceSideJoin.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, FriendMapper.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class, DetailsMapper.class );
		job.setReducerClass(JoinReducer.class);		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
	
		FileOutputFormat.setOutputPath(job,outputDirIntermediate1);
		
	    int code = job.waitForCompletion(true)?0:1;
	    Job job1 = new Job(new Configuration(), "join2");
		job1.setJarByClass(ReduceSideJoin.class);
		FileInputFormat.addInputPath(job1, new Path(args[3] + "_int1"));		
		
		job1.setMapOutputValueClass(Text.class);
		
		job1.setMapperClass(MinAgeMapper.class);
		
		job1.setReducerClass(MinAgeReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job1,outputDirIntermediate2);
		code = job1.waitForCompletion(true) ? 0 : 1;
	
			
	}
}