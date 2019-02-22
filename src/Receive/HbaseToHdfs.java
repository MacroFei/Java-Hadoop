package Receive;



	import java.io.BufferedWriter;
	import java.io.OutputStreamWriter;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.FSDataInputStream;
	import org.apache.hadoop.fs.FSDataOutputStream;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;

	public class HbaseToHdfs {

		/**
		 * 1.	编写一个Java程序，新建一个HDFS文件，并向其中写入你的名字；
		 */
		public static void main(String[] args) {
			try{
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS","hdfs://localhost:9000");
			FileSystem fs = FileSystem.get(conf);
			//chuanjianwancheng
			Path newPath = new Path ("/user/hadoop/newDemo.txt");
			fs.delete(newPath,true);
			FSDataOutputStream os = fs.create(newPath);
			
			String name = "linxuan";

			//FSDataInputStream is = fs.open(path);
			String Bt = "";
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os,"utf-8"));
		
			bw.write(name);
			bw.close();
			os.close();
			//is.close();
			fs.close();
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	}
