package Dispose;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OtherMapReduce {

	public static class LogMapper extends
			Mapper<Object, Text, Text, DoubleWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String array[] = line.split(" ");
			for (int i = 1; i <= 1; i++){
				context.write(new Text(array[0]),
						new DoubleWritable(Integer.parseInt(array[i])));}
		}
	}

	public static class SumReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			Map<String,Double> mapDemo = new HashMap<>();
			for(DoubleWritable val : values ) {
				double  step = val.get();
				int stepNext = (int)(step/10);
				String stepString = stepNext+"";
				
				//key.set(stepString);
				mapDemo.put(stepString, step);
			}
			for (Map.Entry<String, Double> entry : mapDemo.entrySet()) { 
				  System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()); 
				  result .set(entry.getValue());
				  System.out.println(entry.getKey()+entry.getValue()+"asd");
				  Text stepText = null;
				  stepText.set(entry.getKey());
				  context.write( stepText , result);
			}
			
			//context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();


		String[] otherArgs = new String[] { "/Macro/TPAverageDemo.txt",
				"/Macro/output3" };
		//String[] otherArgs = new String[] { "/Macro/output",
				//"/Macro/output4" };
		if (otherArgs.length < 2) {
			System.err.println("Usage: averageScore <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "OtherMapReduce");
		job.setJarByClass(OtherMapReduce.class);
		job.setMapperClass(LogMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// 判断output文件夹是否存在，如果存在则删除
		Path path = new Path(otherArgs[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
