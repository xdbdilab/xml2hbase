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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

import XMarkSearch.XMarkSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query17 {

	private Map<String, List<String>> pctable;
	private String tableName;
	
	public Query17() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query17(String tableName, Map<String, List<String>> pctable){
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
		
		String namePath = "/site/people/person/name";
		String homepagePath = "/site/people/person/homepage";
		
		List<String> finalResult = new ArrayList<String>();
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        List<Filter> filters = new ArrayList<Filter>();
        List<String> nameColumns = new ArrayList<String>();
        List<String> homepageColumns = new ArrayList<String>();
        
        nameColumns.addAll(pctable.get(namePath));
        homepageColumns.addAll(pctable.get(homepagePath));
        pathColumns.addAll(nameColumns);
        pathColumns.addAll(homepageColumns);
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL , new BinaryComparator(Bytes.toBytes(column)));
        	filters.add(filter);
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
        ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
        for(Result result : rs)
        {
        	List<KeyValue> keyValues = result.list();
        	for(int i = 0 ; i < keyValues.size() ;)
        	{
        		String qualifier = Bytes.toString(keyValues.get(i).getQualifier());
        		String value = Bytes.toString(keyValues.get(i).getValue());
        		if(nameColumns.contains(qualifier))
        		{
        			if( (i +1) < keyValues.size())
        			{
        				if(homepageColumns.contains(Bytes.toString(keyValues.get(i+1).getQualifier())))
        				//��һ����homepage
        				{
        					i = i + 2;	//���,������homepage
        					continue;
        				}
        				else{
        					finalResult.add(value);	//������homepage����ӵ������
        				}
        			}
        			else{
        				finalResult.add(value);		//����߽磬֤������Ҳû��homepage
        			}
        			i = i + 1;
        		}else{
        			i = i + 1;
        		}
        	}
        }
        homepageColumns.clear();
        nameColumns.clear();
        rs.close();
        
        //��������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result17", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query17",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * @author QiHaiyang
	 * @time	2016-1-13
	 */
	public static void main(String[] args) {
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		Query17 query = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query17(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}

}
