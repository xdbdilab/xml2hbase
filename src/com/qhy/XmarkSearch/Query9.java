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

public class Query9 {
	
	private Map<String, List<String>> pctable;
	private String tableName;

	
	public Query9() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query9(String tableName, Map<String, List<String>> pctable){
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
		
		String europeId = "/site/regions/europe/item/@id";
		String europeName = "/site/regions/europe/item/name";
		
		Map<String, List<String>> finalResult = new HashMap<String, List<String>>();
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
        rs = null;
        //��ȡEurope�е�id��name��map
        filterList = null;
        filters.clear();
        pathColumns.clear();
        Map<String, String> europeItem = new HashMap<String, String>();
        pathColumns.addAll(pctable.get(europeId));
        pathColumns.addAll(pctable.get(europeName));
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
        	List<KeyValue> keyValues = result.list();
        	for(int i = 0 ; i < keyValues.size() ; i = i + 2)
        	{
        		europeItem.put(Bytes.toString(keyValues.get(i+1).getValue()) 
        					, Bytes.toString(keyValues.get(i).getValue()));		//name��Ϣ������id��Ϣ֮ǰ
        	}
        }
        rs.close();
        
        //�ۺϽ��
        for(String personId : idToID.keySet())
        {
        	List<String> itemIds = idToID.get(personId);
        	for(String id : itemIds)
        	{
        		if(europeItem.containsKey(id))
        		//��Ʒ����Europe
        		{
        			if(finalResult.containsKey(idToName.get(personId)))
        			{
        				finalResult.get(idToName.get(personId)).add(europeItem.get(id));
        			}else {
						List<String> temp = new ArrayList<String>();
						temp.add(europeItem.get(id));	//��ȡ��Ʒ����
						finalResult.put(idToName.get(personId), temp);
						temp = null;
					}
        		}
        	}
        }
        
        //��������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result9", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query9",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
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
		Query9 query = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query9(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}

}
