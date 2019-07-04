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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import XMarkSearch.XMarkSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query16 {
	
	private Map<String, List<String>> pctable;
	private String tableName;
	
	public Query16() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query16(String tableName, Map<String, List<String>> pctable){
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
		
		String keywordPath = "/site/closed_auctions/closed_auction/annotation/description/parlist/listitem/parlist/listitem/text/emph/keyword";
		String idPath = "/site/closed_auctions/closed_auction/seller/@person";
		
		List<String> finalResult = new ArrayList<String>();
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        List<Filter> filters = new ArrayList<Filter>();
        
        pathColumns.addAll(pctable.get(keywordPath));
        pathColumns.addAll(pctable.get(idPath));
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(column)));
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
        		if(qualifier.length() == 17)
        		//�����������Ϣ
        		{
					if ((i + 1) < keyValues.size()) {
						String nextQulifier = Bytes.toString(keyValues.get(
								i + 1).getQualifier());
						if (nextQulifier.length() > 17) {
							finalResult.add("<seller>" + value +"</seller>");
							i = i + 2;
							continue;
						}
					}
					i = i + 1;
        		}else {
					i = i + 1;
				}
        	}
        }
        rs.close();
        //��������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result16", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query16",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
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
		Query16 query = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query16(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}

}
