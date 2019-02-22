package HbaseDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Scanner;

public class ModifyData {

	public static long ts;
	public static Configuration configuration;
	public static Connection connection;
	public static Admin admin;

	public static void modifyData(String tableName, String row, String column,
			String val) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(row.getBytes());
		if (!column.contains(":")) {
			put.addColumn(column.getBytes(), Bytes.toBytes(""), val.getBytes());
		} else {
			String[] cols = column.split(":");
			put.addColumn(cols[0].getBytes(), cols[1].getBytes(),
					val.getBytes());
		}
		table.put(put);
		table.close();
		close();
	}

	public static void init() {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
		try {
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
		} catch (IOException e) {
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
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			System.out.println("请输入表名，行号，列限定符，修改后的数据:");
			System.out.println("例如: MacroDemo  99  Lora_inf:L_HD 77.7");
			Scanner input = new Scanner(System.in);
			String listName = input.next();
			String line = input.next();
			String qualifier = input.next();
			String data = input.next();
			modifyData(listName, line, qualifier, data);
			// modifyData("MacroDemo", "99", "Lora_inf:L_HD","77.7");
			// modifyData("student", "2015001", "stu_inf:s_age", "88");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
