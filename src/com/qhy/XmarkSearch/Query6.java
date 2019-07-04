package com.qhy.XmarkSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import XMarkSearch.XMarkSearch;

import org.apache.hadoop.hbase.filter.RegexStringComparator;

import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query6 {

	private Map<String, List<String>> pctable;
	private String tableName;

	
	public Query6() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query6(String tableName, Map<String, List<String>> pctable){
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
		
		String xpath = "/site/regions/africa/item";
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        pathColumns.addAll(pctable.get(xpath));
        
        int count = 0;
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
        																  columns[6] + "$"));
        	filters.add(filter);
        } 
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
        long time1 = System.currentTimeMillis();
        //��ʼ��ѯ��������
        ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
        for(Result result : rs)
        {
        	count = count + result.size();
        }
        System.out.println("������ݵ�ʱ���ǣ�" + (System.currentTimeMillis() - time1));
        rs.close();
        //��������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result6", "The number is  :" + count);	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query6",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
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
		
		Query6 query = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query6(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}

}
