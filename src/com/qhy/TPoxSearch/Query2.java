package com.qhy.TPoxSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import TPoXSearch.TPoXSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query2 {

	private Map<String, List<String>> pctable;
	private String tableName;
	
	private String securitySymbol = "ATRK";
	
	public Query2() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query2(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	public void query() throws IOException
	{
		long timeTestStart=System.currentTimeMillis();
		String path = "/*[name()='Security']/*[name()='Symbol']";
		
		List<String> finalResult = new ArrayList<String>();
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        List<Filter> filters = new ArrayList<Filter>();
        pathColumns.addAll(pctable.get(path));
        for(String column : pathColumns)
        {
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					  Bytes.toBytes("tpox"),
					  Bytes.toBytes(column),
					  CompareFilter.CompareOp.EQUAL,
					  Bytes.toBytes(securitySymbol)
					  );
			//If the column is missing in a row, filter will skip
			filter.setFilterIfMissing(true);
			filters.add(filter);
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE , filters);
        ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList,null,null);
        byte[] row = null;
        for(Result result : rs)
        {
        	row = result.getRow();
        	System.out.println(Bytes.toString(row));
        }
        System.out.println("Finding over");
        rs.close();
        Result result = HbaseReader.getOneResult(tableName, row);
        List<KeyValue> keyValues  = result.list();
        for(KeyValue keyValue : keyValues)
        {
        	finalResult.add(Bytes.toString(keyValue.getValue()) + "\r\n");
        }
        //�����������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result2", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query2",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @description ��ѯһ
	 * @author QiHaiyang
	 * @time 2016-1-16
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String tableName = "C2V-tpox";
		String P2Ctable = "P2C-tpox";
		Map<String, List<String>> pctable = null;
		Query2 q = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = TPoXSearch.P2C_Map;
			q = new Query2(tableName, pctable);
			for(int i = 0; i < 1; i++){
				q.query();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
