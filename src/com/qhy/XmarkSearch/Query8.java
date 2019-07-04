package com.qhy.XmarkSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import XMarkSearch.XMarkSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query8 {
	
	private Map<String, List<String>> pctable;
	private String tableName;

	
	public Query8() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query8(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	
	/**
	 * @throws IOException 
	 * @description ��ѯ����
	 */
	public void query() throws IOException
	{
		long timeTestStart=System.currentTimeMillis();
		
		String personIDPath = "/site/people/person/@id";
		String personNamePath = "/site/people/person/name";
		
		String buyerPath = "/site/closed_auctions/closed_auction/buyer/@person";
		String itemPath = "/site/closed_auctions/closed_auction/itemref/@item";
		
		Map<String, Integer> finalResult = new HashMap<String, Integer>();
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        List<Filter> filters = new ArrayList<Filter>();
        
        //��ȡ����id�����������map
        pathColumns.addAll(pctable.get(personIDPath));
        pathColumns.addAll(pctable.get(personNamePath));
        Map<String, String> idToName = new HashMap<String, String>();
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL, 
					new BinaryComparator(Bytes.toBytes(column)));
        	filters.add(filter);
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
        ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
        for(Result result : rs)
        {
        	List<KeyValue> keyValues = result.list();
        	for(int i = 0 ; i < keyValues.size() ; i = i + 2)
        	{
        		idToName.put(Bytes.toString(keyValues.get(i+1).getValue()) 
        					, Bytes.toString(keyValues.get(i).getValue()));		//name��Ϣ������id��Ϣ֮ǰ
        	}
        }
        rs.close();
        rs = null;
//      System.out.println(idToName.toString());	//��ȡ������Ϣû�д���
        
        //��ȡperson_id��item_id��map
        filterList = null;
        filters.clear();
        pathColumns.clear();
        Map<String, List<String>> idToID = new HashMap<String, List<String>>();
        pathColumns.addAll(pctable.get(buyerPath));
        pathColumns.addAll(pctable.get(itemPath));
        
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL, 
					new BinaryComparator(Bytes.toBytes(column)));
        	filters.add(filter);
        }
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
        rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
        for(Result result : rs)
        {
        	List<KeyValue> keyValues =  result.list();
        	for(int i = 0 ; i < keyValues.size() ; i = i + 2)
        	{
        		String value = Bytes.toString(keyValues.get(i).getValue());
        		if(idToID.containsKey(value))
        		{
        			idToID.get(value).add(Bytes.toString(keyValues.get(i+1).getValue()));
        		}else{
        			List<String> temp = new ArrayList<String>();
        			temp.add(Bytes.toString(keyValues.get(i+1).getValue()));
        			idToID.put(value, temp);
        			temp = null;
        		}
        	}
        }
        rs.close();
//        System.out.println(idToID.toString());	//��ȡ��Ϣ�޴�
        //�ۺϽ��
        for(String personId : idToID.keySet())
        {
        	finalResult.put(idToName.get(personId), idToID.get(personId).size());
        }
        
        //��������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result8", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query8",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 *@time 2016-1-11
	 *@author QiHaiyang
	 *������
	 */
	public static void main(String[] args) {
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		Query8 query = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query8(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}

}
