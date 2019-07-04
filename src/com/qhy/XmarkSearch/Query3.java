package com.qhy.XmarkSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import XMarkSearch.XMarkSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query3 {
	
	private Map<String, List<String>> pctable;
	private String tableName;

	
	public Query3() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query3(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	
	public void query() throws IOException
	{
		String xpath = "/site/open_auctions/open_auction/bidder/increase";
//		String idpath = "/site/open_auctions/open_auction/@id";
		List<String> finalResult = new ArrayList<String>();
		List<Get> gets = new ArrayList<Get>();
        //Record the start time
        long timeTestStart=System.currentTimeMillis();
        
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ�����1.1.1.1.1����
        pathColumns.addAll(pctable.get(xpath));
        
        //���ù�����
        List<Filter> filters = new ArrayList<Filter>();
        for(String path : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL , new BinaryComparator(Bytes.toBytes(path)));
        	filters.add(filter);
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE , filters);
        
        //��ȡ���
        ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
        
        //������
        for(Result result : rs)
        {
    		int first = 1,last = 1;					//��һ�������һ��increase�ı�־
    		float firstValue = 0,lastValue = 0;		//��һ�������һ��increase��ֵ
    		String parentPath = "";					//��¼·�������ڲ���id
    		int fatherPosition = 1;					//ͬһ���ļ��ж��open_auction,�˱�������ָ��λ��
    		int count = 0;			//���λ��
        	List<KeyValue> keyValues = result.list();
        	for(KeyValue keyValue : keyValues)
        	{
        		parentPath = Bytes.toString(keyValue.getQualifier()).substring(0, 9);
        		String[] qualifier = Bytes.toString(keyValue.getQualifier()).split("[.]");

        		float	value = Float.parseFloat(Bytes.toString(keyValue.getValue()));
        		int position = Integer.parseInt(qualifier[qualifier.length - 3]); 		//ȷ�ϵ�ǰincrease��λ��

        		if(fatherPosition != Integer.parseInt(qualifier[4]))
        		{
					if ( (first != last) && (firstValue * 2 <= lastValue) )
					// ���������������������
					{
						Get get = new Get(result.getRow());
						Filter filter1 = new QualifierFilter(CompareOp.EQUAL, new SubstringComparator(parentPath));
						Filter filter2 = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("open_auction"));
						FilterList fs = new FilterList(filter1,filter2);
						get.setFilter(fs);
						gets.add(get);
					}
					last = 1;
					lastValue = 0;
        		}
        	
				if (position == first) {
					firstValue = value;
				} else if (position > last) {
					lastValue = value;
					last = position;
				} else {
					continue;
				}
        		fatherPosition = Integer.parseInt(qualifier[4]);
        		count = count + 1;
        		if(count == result.size())
        		{
					if ( (first != last) && (firstValue * 2 <= lastValue) )
					// ���������������������
					{
						Get get = new Get(result.getRow());
						Filter filter1 = new QualifierFilter(CompareOp.EQUAL, new SubstringComparator(parentPath));
						Filter filter2 = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("open_auction"));
						FilterList fs = new FilterList(filter1,filter2);
						get.setFilter(fs);
						gets.add(get);
					}
					last = 1;
					lastValue = 0;
        		}
        	}     
        }
        
        //��ȡopen_auction��id
        pathColumns.clear();
        Result[] results = HbaseReader.getByGetList(tableName, gets);
        for(Result result : results)
        {
        	byte[][] value = new byte[result.size()][];
        	result.getFamilyMap(Bytes.toBytes("xmark")).values().toArray(value);
    		for(byte[] a : value){
    			finalResult.add("<ID>" + Bytes.toString(a) + "</ID>");
    		}
        }
        //��������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result3", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query3",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 *@time 2016-1-9  �������ʱ��2016-1-10
	 *@author QiHaiyang
	 *������
	 */
	
	public static void main(String[] args)
	{
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		
		Query3 query3 = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query3 = new Query3(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query3.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}
}
