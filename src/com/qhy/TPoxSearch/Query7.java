package com.qhy.TPoxSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
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

import TPoXSearch.TPoXSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query7 {

	private Map<String, List<String>> pctable;
	private String tableName;
	
	private String id = "1002";
	
	public Query7() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query7(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	public void query() throws IOException
	{
		long timeTestStart=System.currentTimeMillis();
		String idPath = "/*[name()='Customer']/@id";
		String accountIDPath = "/*[name()='Customer']/*[name()='Accounts']/*[name()='Account']/@id";
		String orderAcc = "/*[name()='FIXML']/*[name()='Order']/@Acct";
		String orderCash = "/*[name()='FIXML']/*[name()='Order']/*[name()='OrdQty']/@Cash";
		
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        List<Filter> filters = new ArrayList<Filter>();
        
		pathColumns.addAll(pctable.get(idPath));
        for(String column : pathColumns)
        {
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					  Bytes.toBytes("tpox"),
					  Bytes.toBytes(column),
					  CompareFilter.CompareOp.EQUAL,
					  Bytes.toBytes(id)
					  );
			filter.setFilterIfMissing(true);
			filters.add(filter);
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE , filters);
        ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList,null,null);
        List<String> orderIds = new ArrayList<String>();	//����Account��id
        byte[] row = null;
        for(Result result : rs)
        {
        	row = result.getRow();
        }
        pathColumns.clear();
        filters.clear();
        
        //��ѯAccountID
        pathColumns.addAll(pctable.get(accountIDPath));
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(column)));
        	filters.add(filter);
        }
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE , filters);
        Result result = HbaseReader.getOneResultWithFilters(tableName, row, filterList);
        List<KeyValue> keyValues = result.list();
        for(KeyValue keyValue : keyValues)
        {
        	orderIds.add(Bytes.toString(keyValue.getValue()));
        }
        System.out.println("//��ѯAccountID ���");
        
        //�û�õ�AccountIDȥorder��ƥ��
        rs.close();
        pathColumns.clear();
        filters.clear();
        pathColumns.addAll(pctable.get(orderAcc));
        for(String column : pathColumns)
        {
        	for(String id : orderIds)
        	{
        		System.out.println(id);
    			SingleColumnValueFilter filter = new SingleColumnValueFilter(
  					  Bytes.toBytes("tpox"),
  					  Bytes.toBytes(column),
  					  CompareFilter.CompareOp.EQUAL,
  					  Bytes.toBytes(id)
  					  );
    			filter.setFilterIfMissing(true);
    			filters.add(filter);
        	}
        }
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE , filters);
        rs = HbaseReader.getRowsWithFilterList(tableName, filterList,null,null);
        System.out.println("//�û�õ�AccountIDȥorder��ƥ��");
        
        //���Order��λ��
        List<byte[]> orderList = new ArrayList<byte[]>();
        for(Result tempResult : rs)
        {
        	orderList.add(tempResult.getRow());
        }
        System.out.println(orderList.size() + "");
        System.out.println("//���Order��λ��");
        
        //���Order��Cash
        rs.close();
        filters.clear();
        pathColumns.clear();
        pathColumns.addAll(pctable.get(orderCash));
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(column)));
        	filters.add(filter);
        }
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE , filters);
        List<Get> gets = new ArrayList<Get>();
        for(byte[] orderRow : orderList)
        {
        	Get get = new Get(orderRow);
        	get.setFilter(filterList);
        	gets.add(get);
        }
        Result[] results = HbaseReader.getByGetList(tableName, gets);
        System.out.println("//���Order��Cash");
        
        //�õ�����Cash
        double maxCash = 0.0;
        for(Result tempResult : results)
        {
        	List<KeyValue> tempKeyValues = tempResult.list();
        	for(KeyValue keyValue : tempKeyValues)
        	{
        		double value = Double.parseDouble(Bytes.toString(keyValue.getValue()));
        		if(value > maxCash)
        			maxCash = value;
        	}
        }
        //�����������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result7", maxCash + "");	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query7",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * @description ��ѯһ
	 * @author QiHaiyang
	 * @time 2016-1-17
	 * @param args
	 */
	public static void main(String[] args) {
		String tableName = "C2V-tpox";
		String P2Ctable = "P2C-tpox";
		Map<String, List<String>> pctable = null;
		Query7 q = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = TPoXSearch.P2C_Map;
			q = new Query7(tableName, pctable);
			for(int i = 0; i < 1; i++){
				q.query();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
