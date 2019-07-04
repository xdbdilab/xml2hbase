package XMarkSearch;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.qhy.XmarkSearch.Query1;
import com.qhy.XmarkSearch.Query10;
import com.qhy.XmarkSearch.Query11;
import com.qhy.XmarkSearch.Query12;
import com.qhy.XmarkSearch.Query13;
import com.qhy.XmarkSearch.Query14;
import com.qhy.XmarkSearch.Query15;
import com.qhy.XmarkSearch.Query16;
import com.qhy.XmarkSearch.Query17;
import com.qhy.XmarkSearch.Query18;
import com.qhy.XmarkSearch.Query19;
import com.qhy.XmarkSearch.Query2;
import com.qhy.XmarkSearch.Query20;
import com.qhy.XmarkSearch.Query3;
import com.qhy.XmarkSearch.Query4;
import com.qhy.XmarkSearch.Query5;
import com.qhy.XmarkSearch.Query6;
import com.qhy.XmarkSearch.Query7;
import com.qhy.XmarkSearch.Query8;
import com.qhy.XmarkSearch.Query9;

import cn.edu.xidian.repace.xml2hbase.hbase.HbaseConf;
import coding.xml2hbase.test;
import coding.xml2hbase.hbase.HbaseInsert;


public class XMarkSearch {

	public static HashMap<String, List<String>> P2C_Map = new HashMap<>();	
	
	public static String outputPath = "";
	public static void main(String[] args) {
		HbaseInsert.C2VtableName = "C2V-xmark2.0";
		String filePath = "D:/CaiShunda/测试数据";
		String outPaths = "D:/QiHaiyang/2016-4-15";
		File file = new File(filePath);
		String tableName = "xmark2.0";
		if(file.isDirectory())
		{
			for(String fileName : file.list())
			{
				if(!fileName.contains("xmlgen"))
				{
					continue;
				}
				
				if(fileName.equals("xmlgen1000k"))
				{
					continue;
				}
				
				System.out.println("开始处理" + fileName);
				
				try {
					DeleteAllTable.main(args);
				} catch (IOException e1) {
					e1.printStackTrace();
					System.out.println("delete error");
				}
				
				String[] start = {filePath + "/" + fileName , tableName};	//锟斤拷锟斤拷路锟斤拷
				outputPath = outPaths + "/" + fileName;	//锟斤拷锟铰凤拷锟�
				
				try {
					test.main(start);		//锟斤拷锟斤拷锟斤拷锟�
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("start error");
				}
				
				//寮�濮嬭繘琛屾煡璇㈠伐浣滐紝鐜板皢P2C琛ㄦ斁鍏ュ唴瀛樹腑
				HTable table = null;
				try {
					table = new HTable(HbaseConf.conf, Bytes.toBytes("P2C-xmark2.0"));
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
				
				
				
				
				String[] tableNames = { "C2V-xmark2.0" , "P2C-xmark2.0"};
				for(int i = 0 ; i < 3; i ++)
				{
					Query1.main(tableNames);	sleep();
					Query2.main(tableNames);	sleep();
					Query3.main(tableNames);	sleep();
					Query4.main(tableNames);	sleep();
					Query5.main(tableNames);	sleep();
					Query6.main(tableNames);	sleep();
					Query7.main(tableNames);	sleep();
					Query8.main(tableNames);	sleep();
					Query9.main(tableNames);	sleep();
					Query10.main(tableNames);	sleep();
					Query11.main(tableNames);	sleep();
					Query12.main(tableNames);	sleep();
					Query13.main(tableNames);	sleep();
					Query14.main(tableNames);	sleep();
					Query15.main(tableNames);	sleep();
					Query16.main(tableNames);	sleep();
					Query17.main(tableNames);	sleep();
					Query18.main(tableNames);	sleep();
					Query19.main(tableNames);	sleep();
					Query20.main(tableNames);	sleep();
				}
				System.out.println(fileName + "  id done");
				P2C_Map.clear();
			}
		}
	}

	
	public static void sleep()
	{
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
