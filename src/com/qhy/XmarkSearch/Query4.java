package com.qhy.XmarkSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import XMarkSearch.XMarkSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseConf;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query4 {
	
	private Map<String, List<String>> pctable;
	private String tableName;

	private String person1 = "person23141";
	private String person2 = "person6618";
	
	public Query4() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query4(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	/**
	 * @throws IOException 
	 * @description ��ѯ����
	 */
	public void query() throws IOException
	{
		long timeTestStart=System.currentTimeMillis();		//��ʼʱ��
		
		List<String> finalResult = new ArrayList<String>();
		HTable table = new HTable(HbaseConf.conf, tableName);
		
		Filter filter = new ValueFilter(CompareOp.EQUAL, new RegexStringComparator( "(" + person1 +"|" + person2 + ")" ));
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		//��ѯ��������
		ResultScanner rs = table.getScanner(scan);
		for(Result result : rs)
		{
			int position = 1;	//open_auction��λ�ã���ʼΪ1
			int personPosition1 = 0 , personPosition2 = 0;	//��¼person��λ�ã�Ϊ0ʱ��ʾû���ҵ�
			int count = 0;		//���������
			List<KeyValue> keyValues = result.list();
			for(KeyValue keyValue : keyValues)
			{
				String[] qualifier = Bytes.toString(keyValue.getQualifier()).split("[.]");
				String value = Bytes.toString(keyValue.getValue());
				int rightPositon = Integer.parseInt(qualifier[6]); 		//��ǰλ��
				
				if(position != Integer.parseInt(qualifier[4]))
				//�ƶ�����һ��open_auction��ʱ��
				{
					if( (personPosition1 != 0) && (personPosition2 != 0) )
					//�Ѿ�ȫ���ҵ�
					{
						if(personPosition1 < personPosition2)
						//�õ����
						{
							finalResult.add(Bytes.toString(result.getRow()) + "    ");
						}
					}
					personPosition1 = 0 ;
					personPosition2 = 0;
				}//end if
				
				if(value.equals(person1))
				{
					personPosition1 = rightPositon;
				}else if (value.equals(person2)) {
					personPosition2 = rightPositon;
				}else{
					continue;
				}
				
				position = Integer.parseInt(qualifier[4]);
				count = count + 1;
				
				if(count == result.size())
				{
					if( (personPosition1 != 0) && (personPosition2 != 0) )
					//�Ѿ�ȫ���ҵ�
					{
						if(personPosition1 < personPosition2)
						//�õ����
						{
							finalResult.add(Bytes.toString(result.getRow()) + "    ");
						}
					}
					personPosition1 = 0 ;
					personPosition2 = 0;
				}
				
			}//end for(KeyValue keyValue : keyValues)
		}//end for(Result result : rs)
		
		table.close();
		rs.close();
        //��������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result4", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query4",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		System.out.println(finalResult.toString());
	}
	
	
	/**
	 *@time 2016-1-10
	 *@author QiHaiyang
	 *������
	 */
	public static void main(String[] args)
	{
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		
		Query4 query = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query4(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}
}
