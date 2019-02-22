package HadoopDemo;

import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class Demo5 {
	/**
	 * 1. 编写一个Java程序，实现：给定HDFS中某一个目录， 输出该目录下的所有文件的读写权限、大小、创建时间、路径等信息，
	 * 如果该文件是目录，则递归输出该目录下所有文件相关信息。（注意与上一题的区别）
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        String remoteDir = "user/hadoop";
        System.out.println("(digui)find all file's message:" + remoteDir);
        try{
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(remoteDir);
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(dirPath, true);
        while(remoteIterator.hasNext()){      
        	FileStatus s = remoteIterator.next();
        	System.out.println("url"+s.getPath().toString());
        	System.out.println("quanxing"+s.getPermission().toString());
        	System.out.println("size: "+ s.getLen());
        	
        	Long timeStamp = s.getModificationTime();
        	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        	String date = format.format(timeStamp);
        	System.out.println("time"+date);
        	System.out.println();
        	   }
        }catch(Exception e){
        	e.printStackTrace();
        }
	}
}

//  String remoteFilePath = "/user/tiny/input/text.txt"; // HDFS路径
//  String remoteDir = "/user/test/input"; // HDFS路径对应的目录
