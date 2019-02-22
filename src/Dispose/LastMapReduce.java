package Dispose;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LastMapReduce {
//	 public static class MyMapper
//     extends Mapper<Object, Text, IntWritable ,Text>{
// 
//     //private final static IntWritable one = new IntWritable(1);
//   
//  public void map(Object key, Text value, Context context
//                  ) throws IOException, InterruptedException {
//    String line =value.toString();
//   String array[]=line.split("\t");
//   int keyOutput=Integer.parseInt(array[1]);
//   String valuOutput=array[0];
//   context.write(new IntWritable(keyOutput), new Text(valuOutput));
//      }
//  }
//     public static class MyReducer
//     extends Reducer<IntWritable,Text,Text,IntWritable> {
//     //private IntWritable result = new IntWritable();
//
//  public void reduce(IntWritable key, Iterable<Text> values,
//                     Context context
//                     ) throws IOException, InterruptedException {
//    for (Text value : values) {
//    context.write(value, key);
//  }
//}
//
//     public static void main(String[] args) throws Exception {
//          Configuration conf = new Configuration();
//          //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//          String[] otherArgs = new String[] {"/Macro/output","/Macro/LastOutput"};
//          if (otherArgs.length < 2) {
//            System.err.println("Usage: acessTimeSort <in> [<in>...] <out>");
//            System.exit(2);
//          }
//          Job job = new Job(conf, "LastMapReduce");
//          job.setJarByClass(LastMapReduce.class);
//          job.setMapperClass(MyMapper.class);
//          job.setReducerClass(MyReducer.class);
//          job.setMapOutputKeyClass(IntWritable.class);
//          job.setMapOutputValueClass(Text.class);
//          job.setOutputKeyClass(IntWritable.class);
//          job.setOutputValueClass(Text.class);
//          for (int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//          }
//          FileOutputFormat.setOutputPath(job,
//            new Path(otherArgs[otherArgs.length - 1]));
//          System.exit(job.waitForCompletion(true) ? 0 : 1);
//        }
//        }

}
