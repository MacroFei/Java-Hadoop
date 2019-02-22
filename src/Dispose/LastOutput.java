package Dispose;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LastOutput {
	public static void main(String[] args) {
	try {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(conf);
		Scanner input = new Scanner(System.in);
		System.out.println("请输入文件目录，例如：");
		System.out.println("/user/hadoop/testdemo2.txt");
		// "/Macro/MacroDemo2.txt"
		String pathDemo = input.nextLine();
		Path path = new Path(pathDemo);
		// Path path = new Path("/user/hadoop/testdemo2.txt");
		// Path newPath = new Path ("");
		FSDataInputStream is = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(is,
				"utf-8"));
		String line = "";
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}
		br.close();
		is.close();
		fs.close();

	} catch (Exception e) {
		e.printStackTrace();
	}

}

}
