package HadoopDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Mkdir {

	/**
	 * 1.	编写一个Java程序，实现：提供一个HDFS内的文件的路径，对该文件进行创建和删除操作。如果文件所在目录不存在，则自动创建目录。

	 */
	public static boolean test(Configuration conf , String path){
		
		try (FileSystem fs = FileSystem.get(conf)){
			return fs.exists(new Path(path));
		} catch (IOException e) 
		{	
			e.printStackTrace();
			return false;
		}		
	}
	public static boolean mkdir (Configuration conf , String remoteDir){
		try(FileSystem fs = FileSystem.get(conf)){
			Path dirPath = new Path (remoteDir);
			return fs.mkdirs(dirPath);
		}catch(IOException e){
			e.printStackTrace();
			return false;
		}
	}
	public static void touchz(Configuration conf , String remoteFilePath){
		Path remotePath = new Path (remoteFilePath);
		try(FileSystem fs = FileSystem.get(conf)){
			FSDataOutputStream outputStream = fs.create(remotePath);
			outputStream.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		String remoteFilePath = "/user/mac/input/text.txt";
		String remoteDir = "/user/mac/input";	
		try{		
				if(!Mkdir.test(conf,remoteDir)){
					Mkdir.mkdir(conf,remoteDir);
				}
				Mkdir.touchz(conf,remoteFilePath);
				System.out.println("new file:" + remoteFilePath);
		
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
