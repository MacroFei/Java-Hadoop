package Dispose;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DisposeDemo {// 将文件从Hbase to Hdfs
	//public static int averageDemo = 0;
	public static List<String> listDemo = new ArrayList<String>();

	public static void main(String[] args) {
		try {
			System.out.println("将文件从Hbase to Hdfs.");
			System.out.println("请输入数据库表名:	(例如：MacroDemo)");
			Scanner input = new Scanner(System.in);
			String listName = input.nextLine();
			System.out.println("请输入列族：列限定符  例如：Lora_inf:L_TP");
			String listvalue = input.nextLine();
			ScanColumn.scanColumn(listName, listvalue);

		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://localhost:9000");
			FileSystem fs = FileSystem.get(conf);
			// chuanjianwancheng
			Path newPath = new Path("/Macro/newDemo.txt");
			fs.delete(newPath, true);
			FSDataOutputStream os = fs.create(newPath);

			// String name = "linxuan";

			// FSDataInputStream is = fs.open(path);
			String Bt = "";
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os,
					"utf-8"));
			for (int i = 1; i < listDemo.size(); i++) {
				bw.write(listDemo.get(i) + "\n");
			//	averageDemo += Integer.parseInt(listDemo.get(i));
			}
			//averageDemo /= listDemo.size();
			//System.out.print(averageDemo+"hello world");
			bw.close();
			os.close();
			// is.close();
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class ScanColumn {
		public static Configuration configuration;
		public static Connection connection;
		public static Admin admin;

		public static void scanColumn(String tableName, String column)
				throws IOException {
			init();
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();

			if (!column.contains(":")) {
				scan.addFamily(Bytes.toBytes(column));
			} else {
				String[] cols = column.split(":");
				scan.addColumn(Bytes.toBytes(cols[0]), Bytes.toBytes(cols[1]));
			}
			ResultScanner scanner = table.getScanner(scan);
			for (Result result = scanner.next(); result != null; result = scanner
					.next()) {
				showCell(result);
			}
			table.close();
			close();
		}

		public static void showCell(Result result) {
			Cell[] cells = result.rawCells();
			for (Cell cell : cells) {
				String valueDemo = new String(CellUtil.cloneValue(cell));
				listDemo.add(valueDemo);
				// System.out.println("RowKey:"
				// + new String(CellUtil.cloneRow(cell)) + "    Timetamp:"
				// + cell.getTimestamp() + "    columnFamily:"
				// + new String(CellUtil.cloneFamily(cell))
				// + "    ColumnName:"
				// + new String(CellUtil.cloneQualifier(cell))
				// + "   value:" + valueDemo);
				System.out.println("value:" + valueDemo);

			}
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

		// 关闭连接
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

	}
}
