package HadoopDemo;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Rm {

	/**
	 * 1. 编写一个Java程序，实现：提供一个HDFS内的文件的路径，对该文件进行创建和删除操作。
	 */
	public static boolean test(Configuration conf, String path) {

		try (FileSystem fs = FileSystem.get(conf)) {
			return fs.exists(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static void touchz(Configuration conf, String remoteFilePath) {
		Path remotePath = new Path(remoteFilePath);
		try (FileSystem fs = FileSystem.get(conf)) {
			FSDataOutputStream outputStream = fs.create(remotePath);
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static boolean rm(Configuration conf, String remoteFilePath) {
		Path remotePath = new Path(remoteFilePath);
		try (FileSystem fs = FileSystem.get(conf)) {
			return fs.delete(remotePath, false);

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		Scanner input = new Scanner(System.in);
		System.out.println("请输入需要删除文件的绝对路径");
		System.out.println("例如：/user/mac/input/text.txt");
		String remoteFilePath = input.nextLine();
		// String remoteFilePath = "/user/mac/input/text.txt";
		// String remoteDir = "/user/mac/input";

		try {
			if (Rm.test(conf, remoteFilePath)) {
				Rm.rm(conf, remoteFilePath);
				System.out.println("delete " + remoteFilePath);

			} else {
				System.out.println("删除失败");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
