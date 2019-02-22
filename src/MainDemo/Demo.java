package MainDemo;

import java.io.IOException;
import java.util.Scanner;

import HbaseDemo.Hbaseexample;

public class Demo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		while (true) {
			System.out.println("请选择功能:");
			System.out.println("1.Hadoop 文件管理功能（增，删，查）");
			System.out.println("2.数据库管理功能");
			System.out.println("3.智能农业主界面");
			System.out.println("4.退出系统");
			Scanner input = new Scanner(System.in);
			int chioce = input.nextInt();
			switch (chioce) {
			case 1:
				System.out.println("请选择功能:");
				System.out.println("1.文件上传");
				System.out.println("2.文件删除");
				System.out.println("3.文件查询");
				System.out.println("4.返回主界面");
				int chioceDemo1 = input.nextInt();
				switch (chioceDemo1) {
				case 1:
					HadoopDemo.UploadDemo.main(args);
					break;
				case 2:
					HadoopDemo.Rm.main(args);
					break;
				case 3:
					HadoopDemo.ReadFile.main(args);
					break;
				case 4:
					MainDemo.Demo.main(args);
					break;
				}
				break;

			case 2:
				while (true) {
					System.out.println("请选择功能:");
					System.out.println("1.数据库修改");//
					System.out.println("2.文件输入数据库");// xieside
					System.out.println("3.数据库查询,并输出到文件");

					System.out.println("4.返回主界面");

					int chioceDemo2 = input.nextInt();
					switch (chioceDemo2) {
					case 1:
						HbaseDemo.ModifyData.main(args);
						break;
					case 2:
						Receive.ReceiveDemo.main(args);
						break;
					case 3:
						Dispose.DisposeDemo.main(args);
						break;
					case 4:
						MainDemo.Demo.main(args);
						break;
					}
				}

			case 3:// 使用该功能前先删除数据表
				while (true) {
					System.out
							.println("1.模拟接收数据（直接将Hadoop上的TXT文件中的内容存到Hbase中，并自动创建表）");
					System.out.println("2.MapReduce处理数据");
					System.out.println("3.以JSON格式发送数据/控制命令");
					System.out.println("4.返回上级界面");
					int chioceDemo3 = input.nextInt();
					switch (chioceDemo3) {
					case 1:
						Receive.ReceiveDemo.main(args);
						break;
					case 2:
						try {
							Dispose.DisposeMapReduce.main(args);
						} catch (Exception e) {
							System.err.println("MapReduce异常");
							e.printStackTrace();
						}
						break;
					case 3:
						Transmit.TransmitDemo.main(args);
						break;
					case 4:
						MainDemo.Demo.main(args);
						break;
					}
				}

			case 4:
				System.out.println("退出系统,886");
				System.exit(0);
			}
		}
	}
}
