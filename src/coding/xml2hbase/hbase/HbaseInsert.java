package coding.xml2hbase.hbase;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

//import cn.edu.xidian.repace.xml2hbase.MappingTable;

public class HbaseInsert {
	public static long count=0;
	public static String C2VtableName = "C2V-tpox"; // �޸�3
	static List<Put> putList = new ArrayList<Put>(); // ʹ�� List of Puts
	static Put put = null;
	static HTable C2Vtable = null;
	static Map<String, String> code2Value = new HashMap<String, String>();
	static {
		try {
			C2Vtable = new HTable(HbaseConf.conf, C2VtableName);
			C2Vtable.setAutoFlush(false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void addRecord(String P2CtableName, Map<String, List<String>> path2Code, String family) {
		// System.out.println("begin to insert data to path2Code");
		if (!P2CtableName.equals("")) {
			try {

				HTable table = new HTable(HbaseConf.conf, P2CtableName);
				Put put = new Put(Bytes.toBytes(P2CtableName));// row key
																// =P2CtableName
				put.setWriteToWAL(false);
				String qualifier = "";
				String Codes = "";

				for (Iterator i = path2Code.keySet().iterator(); i.hasNext();) {
					Codes = "";
					qualifier = i.next().toString();
					List<String> list = path2Code.get(qualifier);

					for (int j = 0; j < list.size(); j++) {

						String s = list.get(j);
						Codes = Codes + s + "#";
					}

					put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(Codes));
				}

				table.put(put);
			} catch (IOException e) {
				System.out.println(e.getMessage() + "record insertion failed");
			}

		}

	}

	public static void addRecord(String C2VtableName, String family, Map<String, Map<String, String>> code2Values) {
		System.out.println("begin to insert data to code2value");
		if (!C2VtableName.equals("")) {
			try {
				String qualifier = "";
				String Value = "";
				String rowName = "";

				for (Iterator<String> i = code2Values.keySet().iterator(); i.hasNext();) {
					
					rowName = i.next().toString(); // rowname
					code2Value = code2Values.get(rowName);    //此处添加负载均衡
					
					TimeStamp ts = new TimeStamp(0);
					Timestamp d = new Timestamp(System.currentTimeMillis());
					if(d.hashCode() > 0)
					{
						rowName = "R" + d.hashCode()%200 + "-" + rowName;
					}else{
						rowName = "R" + (d.hashCode()*(-1))%200 + "-" + rowName;
					}
					put = new Put(Bytes.toBytes(rowName));
					put.setWriteToWAL(false);
					//tianjia
					count=count+code2Values.size(); 
					
					for (Iterator<String> j = code2Value.keySet().iterator(); j.hasNext();) {
						qualifier = j.next().toString();
						Value = code2Value.get(qualifier);
						put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(Value));

					}
					putList.add(put);
					code2Value = new HashMap<String, String>();
				}
				C2Vtable.put(putList);
				C2Vtable.flushCommits();
				putList.clear();
			} catch (IOException e) {
				System.out.println(e.getMessage() + "record insertion failed");
			}
		} // if
		else {
			System.out.println("C2VtableName is empty");
		}

	}

	/*
	 * public static void closeHTablePool(String C2VtableName) throws
	 * IOException { for(int i =0;i<5;i++) { tablePool.putTable(tables[i]); }
	 * tablePool.closeTablePool(C2VtableName);
	 * 
	 * }
	 */

}
