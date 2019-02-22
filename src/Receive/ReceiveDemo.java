package Receive;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import ClassDemo.Lora;

public class ReceiveDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("接收数据");

		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://localhost:9000");
			FileSystem fs = FileSystem.get(conf);
			Scanner input = new Scanner(System.in);
			System.out.println("请输入文件目录，例如：");
			System.out.println("/Macro/MacroDemo2.txt");
			String pathDemo = input.nextLine();
			Path path = new Path(pathDemo);
			// Path path = new Path("/user/hadoop/testdemo2.txt");
			// Path newPath = new Path ("");
			FSDataInputStream is = fs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(is,
					"utf-8"));
			String line = "";

			String[] fields1 = { "Lora_inf" };
			try {
				CreateTable.createTable("MacroDemo", fields1);
			} catch (IOException e) {
				System.err.print("create Table error");
				e.printStackTrace();
			}

			// List<Lora> list = new ArrayList<Lora>();
			int i = 1;
			String s = "";
			s = i + "";
			while ((line = br.readLine()) != null) {
				String[] fields2 = { "Lora_inf:L_TP", "Lora_inf:L_HD",
						"Lora_inf:CO2" };
				if (line.contains(" ")) {
					String[] cols = line.split(" ");
					String[] values1 = { cols[0], cols[1], cols[2] };
					try {
						CreateTable.addRecord("MacroDemo", s, fields2, values1);
						// addRecord("MacroDemo",s,fields2,values1);
						// addRecord("Macro", i, fields1, values1);
						i++;
						s = i + "";
					} catch (IOException e) {
						System.err.println("addRecord添加数据异常");
						e.printStackTrace();
					}
				}

			}
			br.close();
			is.close();
			fs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class CreateTable {
		public static Configuration configuration;
		public static Connection connection;
		public static Admin admin;

		public static void createTable(String tableName, String[] fields)
				throws IOException {
			init();
			TableName tablename = TableName.valueOf(tableName);
			if (admin.tableExists(tablename)) {
				System.out.println("table is exists!");
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			}
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tablename);
			for (String str : fields) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
			admin.createTable(hTableDescriptor);
			close();
		}

		public static void init() {
			configuration = HBaseConfiguration.create();
			configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
			try {
				connection = ConnectionFactory.createConnection(configuration);
				admin = connection.getAdmin();
			} catch (IOException e) {
				System.err.print("init error");
				e.printStackTrace();
			}
		}

		public static void close() {
			try {
				if (admin != null) {
					admin.close();
				}
				if (null != connection) {
					connection.close();
				}
			} catch (IOException e) {
				System.err.print("close error");
				e.printStackTrace();
			}
		}

		public static void addRecord(String tableName, String row,
				String[] fields, String[] values) throws IOException {
			init();
			Table table = connection.getTable(TableName.valueOf(tableName));
			for (int i = 0; i != fields.length; i++) {
				Put put = new Put(row.getBytes());
				String[] cols = fields[i].split(":");
				put.addColumn(cols[0].getBytes(), cols[1].getBytes(),
						values[i].getBytes());
				table.put(put);
			}
			table.close();
			close();
		}
	}
}
