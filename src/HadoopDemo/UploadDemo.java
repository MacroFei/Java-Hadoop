package HadoopDemo;

import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UploadDemo {

	/**
	 * 1.编写一个Java程序，将一个本地文件上传到HDFS中并改名，
	 */
	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://localhost:9000");//
			FileSystem fs = FileSystem.get(conf);

			System.out.println("请输入需要上传的文件路径");
			System.out.println("例如： /home/mac/test/test1.txt");
			// youwenti
			Scanner input = new Scanner(System.in);
			String fromPathDemo = input.nextLine();
			Path fromPath = new Path(fromPathDemo);
			System.out.println("请输入文件上传到的路径");
			System.out.println("例如： /user/hadoop/testdemo2.txt");
			String toPathDemo = input.nextLine();
			Path toPath = new Path(toPathDemo);
			fs.copyFromLocalFile(fromPath, toPath);
			System.out.println("传输完成");
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}