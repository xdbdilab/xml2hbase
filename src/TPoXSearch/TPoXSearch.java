package TPoXSearch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.qhy.TPoxSearch.Query1;
import com.qhy.TPoxSearch.Query2;

import com.qhy.TPoxSearch.Query3;
import com.qhy.TPoxSearch.Query4;
import com.qhy.TPoxSearch.Query5;
import com.qhy.TPoxSearch.Query6;
import com.qhy.TPoxSearch.Query7;
import com.qhy.TPoxSearch.WriteRecord;

import XMarkSearch.DeleteAllTable;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseConf;
import coding.xml2hbase.tpoxTest;
import coding.xml2hbase.hbase.HbaseInsert;

public class TPoXSearch {

	public static HashMap<String, List<String>> P2C_Map = new HashMap<>();
	public static String outputPath = "";

	public static void main(String[] args) {
		HbaseInsert.C2VtableName = "C2V-tpox";
		String filePath = "D:/CaiShunda/测试数据";
		String outPaths = "D:/QiHaiyang/2016-4-15";
		File file = new File(filePath);
		String tableName = "tpox";
		if (file.isDirectory()) {
			for (String fileName : file.list()) {
				if (!fileName.contains("tpox")) {
					continue;
				}
				if(fileName.contains("tpox1batch") || fileName.equals("tpox100batch"))
				{
					continue;
				}
				
				try {
					DeleteAllTable.main(args);
				} catch (IOException e1) {
					e1.printStackTrace();
					System.out.println("delete error");
				}

				String[] start = { filePath + "/" + fileName, tableName }; // ����·��
				outputPath = outPaths + "/" + fileName; // ���·��
				System.out.println("start  :" + fileName);
				try {
					tpoxTest.main(start); // ��������
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("start error");
				}
				
				HTable table = null;
				try {
					table = new HTable(HbaseConf.conf, Bytes.toBytes("P2C-tpox"));
					Scan scan = new Scan();
					ResultScanner rs = table.getScanner(scan);
					for(Result result : rs)
					{
						for(KeyValue keyValue : result.list())
						{
							List<String> tempList = new ArrayList<>();
							String[] temp = Bytes.toString(keyValue.getValue()).split("#");
							for(String string : temp)
							{
								tempList.add(string);
							}
							P2C_Map.put(Bytes.toString(keyValue.getQualifier()), tempList);
							tempList = null;
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				String[] tableNames = { "C2V-tpox", "P2C-tpox" };
				
				try{
					WriteRecord.Record("记录数量", "记录数量："+HbaseInsert.count);
				}catch(IOException e){
					e.printStackTrace();
				}
				
				for (int i = 0; i < 5; i++) {
					Query1.main(tableNames);
					sleep();
					Query2.main(tableNames);
					sleep();
					Query3.main(tableNames);
					sleep();
					Query4.main(tableNames);
					sleep();
					Query5.main(tableNames);
					sleep();
					Query6.main(tableNames);
					sleep();
					Query7.main(tableNames);
					sleep();
				}
				
				HbaseInsert.count=0;
				
				System.out.println(fileName + "  id done");
			}
		}
	}

	public static void sleep() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
