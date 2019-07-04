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
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import XMarkSearch.XMarkSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query19 {

	private Map<String, List<String>> pctable;
	private String tableName;
	
	public Query19() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query19(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	
	public void query() throws IOException
	{
		long timeTestStart=System.currentTimeMillis();
		
		String xpath = "/site/regions/africa/item/name";
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        Map<String, List<String>> finalResult = new HashMap<String, List<String>>();
        pathColumns.addAll(pctable.get(xpath));
        List<Filter> filters = new ArrayList<Filter>();
        
        for(int i = 0 ; i < pathColumns.size() ; i++)
        {
        	String[] columns = pathColumns.get(i).split("[.]");
        	Filter filter = new QualifierFilter(CompareOp.EQUAL , 
        										new RegexStringComparator("^" + columns[0] + "+[.]" +
        																  columns[1] + "+[.]" +
        																  columns[2] + "+[.]" +
        																  "[0-9]+[.]"  +
        																  columns[4] + "+[.]" +
        																  columns[5] + "+[.]" +
        																  columns[6] + "+[.]" +
        																  columns[7] + "+[.]" +
        																  columns[8] +"$"));
        	filters.add(filter);
        } 
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
        //�õ���Ʒ��ƣ�������
        Map<String, List<String>> locationToNames = new HashMap<String, List<String>>();
        ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
        for(Result result : rs)
        {
        	List<KeyValue> keyValues = result.list();
        	for(KeyValue keyValue : keyValues)
        	{
        		String key = Bytes.toString(keyValue.getQualifier()).substring(0, 9);
        		String value = Bytes.toString(keyValue.getValue());
        		if(locationToNames.containsKey(key))
        		{
        			locationToNames.get(key).add(value);
        		}else{
        			List<String> temp = new ArrayList<String>();
        			temp.add(value);
        			locationToNames.put(key, temp);
        			temp = null;
        		}
        	}
        }
        rs.close();
        filters.clear();
        
        //�õ�����ĵ������
        for(String location : locationToNames.keySet())
        {
//        	System.out.println(location);
        	Filter filter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(location+"#")));
        	filters.add(filter);
        }
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
        rs = HbaseReader.getRowsWithFilterList("P2C-xmark2.0", filterList, null, null);
        for(Result result : rs)
        {
        	List<KeyValue> keyValues = result.list();
        	System.out.println("Coming in ");
        	for(KeyValue keyValue : keyValues)
        	{
//        		System.out.println(Bytes.toString(keyValue.getValue()));
        		String[] qualifier = Bytes.toString(keyValue.getQualifier()).split("/");
        		String value = Bytes.toString(keyValue.getValue());
        		finalResult.put(qualifier[qualifier.length - 1], locationToNames.get(value.substring(0, value.length()-1)));
        	}
        }
        rs.close();
//      WriteRecord.Record("Search19-temp", locationToNames.toString());
        //��������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result19", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query19",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finalResult = null;
		locationToNames.clear();
	}
	
	/**
	 * @author QiHaiyang
	 * @time	2016-1-13
	 */
	public static void main(String[] args) {
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		Query19 query = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query19(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}

}
