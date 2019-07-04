package com.qhy.XmarkSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import XMarkSearch.XMarkSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query5 {

	private Map<String, List<String>> pctable;
	private String tableName;

	
	public Query5() {
		pctable = null;
		tableName = null;
	}
	
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query5(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	/**
	 * @description ��ѯ����
	 * @throws IOException
	 */
	public void query() throws IOException
	{
		long timeTestStart=System.currentTimeMillis();
		String xpath = "/site/closed_auctions/closed_auction/price";
		List<String> columns = pctable.get(xpath);
		int count = 0;	//��¼���
		if(columns != null)
		{
			List<Filter> filters = new ArrayList<Filter>();
			for(int i = 0; i < columns.size(); ++i){
				//New a SingleColumnValueFilter to filter on a qualifier
				Filter filter = new QualifierFilter(CompareOp.EQUAL, 
						new BinaryComparator(Bytes.toBytes(columns.get(i))));
				//Add the filter into filterList
				filters.add(filter);
			}
			
			//��ʼ��ѯ���������
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
			ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList,null,null);
			for(Result result : rs)
			{
				List<KeyValue> keyValues = result.list();
				for(KeyValue keyValue : keyValues)
				{
					float value = Float.parseFloat(Bytes.toString(keyValue.getValue()));
					if(value > 40)
					{
						count = count + 1;
					}
				}
			}
			
	        //��������¼ʱ��
	        long timeTestExcute = System.currentTimeMillis();
	        WriteRecord.Record("Result5", "The number is:   " + count);	//������
			try {
				 Date dt=new Date();
			     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
				WriteRecord.Record("Query5",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}//end if
	}
	
	
	/**
	 *@time 2016-1-10
	 *@author QiHaiyang
	 *������
	 */
	public static void main(String[] args) {
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		
		Query5 query = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query5(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}
	
}
