package coding.xml2hbase;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import coding.xml2hbase.MappingTableManager;
import coding.xml2hbase.VirtualTable;
import coding.xml2hbase.hbase.*;

public class tpoxTest {
	public static void main(String[] args) throws Exception {
		MappingTableManager MTmanager = new MappingTableManager();

		Map<String, Map<String, String>> code2Values = new HashMap<String, Map<String, String>>();
		Map<String, String> code2Value = new HashMap<String, String>();
		Map<String, List<String>> path2Code = new HashMap<String, List<String>>();
		String tableName = args[1]; // �޸�1 2016-1-8������
		String family = "tpox";
		String P2CtableName = "P2C-" + tableName;
		String C2VtableName = "C2V-" + tableName;
		HbaseCreate.createTable(P2CtableName, family);
		HbaseCreate.createTable(C2VtableName, family);

		// open files,creating the map
		String C2ProwName = "";
		VirtualTable vt = new VirtualTable(P2CtableName);
		int fileCount = 0;
		int fileNumber = 1000; // fileNumber=300

		File file = new File(args[0]); 								// �޸�2 2016-1-8 ���ļ�·��
		if (file.isDirectory()) { 									// ��һ���ļ�Tpox���ļ��й�������
			File[] files = file.listFiles();
			for (File file2 : files) { 								// �ڶ����ļ��ļ���
				File[] files2 = file2.listFiles();
				for (File file3 : files2) { 						// ���һ���ļ���
					File[] files3 = file3.listFiles();
					for (fileCount = 0; fileCount < files3.length;) {	//��λ���ļ�
						if ((fileNumber + fileCount) < files3.length) {
							System.out.println("yes" + (fileNumber + fileCount));
							while (fileNumber > 0) {
								String path = files3[fileCount].getPath();// article
																			// path
								C2ProwName = files3[fileCount].getName();// article
																			// name
																			// as
																			// the
																			// C2VrowName
								MTmanager.createMappingTable(path, C2ProwName,vt, path2Code, code2Value);
								vt.flushCurrentNumber();
								code2Values.put(C2ProwName, code2Value);
								code2Value = null;
								code2Value = new HashMap<String, String>();
								fileNumber--;
								fileCount++;
							}
							HbaseInsert.addRecord(C2VtableName, family,code2Values);
							code2Values.clear();
							fileNumber = 1000; // fileNumber=300;
						}
						if ((fileNumber + fileCount) >= files3.length) {
							while (fileCount < files3.length) {
			        			String path = files3[fileCount].getPath();//article path 
					        	C2ProwName =  files3[fileCount].getName();//article name as the C2VrowName
					        	code2Value.clear();
					        	MTmanager.createMappingTable(path,C2ProwName,vt,path2Code,code2Value);
					        	
					        	vt.flushCurrentNumber();
					        	code2Values.put(C2ProwName, code2Value);	
					        	code2Value = null;
					        	code2Value = new HashMap<String, String>();
								fileCount++;
							}
							HbaseInsert.addRecord(C2VtableName, family,code2Values);
							code2Values.clear();
						}
					}//��λ���ļ�����
//					HbaseInsert.addRecord(P2CtableName, path2Code, family);	//������һ���ļ���Ӧ�ý��д洢
					fileCount = 0;	fileNumber = 1000;				//���³�ʼ������
				}//���һ���ļ���
				HbaseInsert.addRecord(P2CtableName, path2Code, family);		//������һ���ļ��󣬴���һ��,������Ϣ����
				path2Code.clear();
			}//�ڶ����ļ���
//			HbaseInsert.addRecord(P2CtableName, path2Code, family);
		} //��һ���ļ���
		if (file.isFile()) {
			String path = file.getPath();
			C2ProwName = file.getName();// article name as the C2VrowName
			code2Value.clear();
			MTmanager.createMappingTable(path, C2ProwName, vt, path2Code,
					code2Value);
			code2Values.put(C2ProwName, code2Value);
			HbaseInsert.addRecord(C2VtableName, family, code2Values);
			HbaseInsert.addRecord(P2CtableName, path2Code, family);
			// code2Value.clear();
		}
	}
}
