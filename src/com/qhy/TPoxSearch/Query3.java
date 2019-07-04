package com.qhy.TPoxSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.xml.crypto.Data;

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

import TPoXSearch.TPoXSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query3 {

	private Map<String, List<String>> pctable;
	private String tableName;
	
	private String custaccID = "1002";
	
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
		long timeTestStart=System.currentTimeMillis();
		String path = "/*[name()='Customer']/@id";
		
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
					  Bytes.toBytes(custaccID)
					  );
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
        rs.close();
        filters.clear();
        pathColumns.clear();
        String titlePath = "/*[name()='Customer']/*[name()='Name']/*[name()='Title']";				pathColumns.addAll(pctable.get(titlePath));
        String firstNamePath = "/*[name()='Customer']/*[name()='Name']/*[name()='FirstName']";		pathColumns.addAll(pctable.get(firstNamePath));
        String middleNamePath = "/*[name()='Customer']/*[name()='Name']/*[name()='MiddleName']";	pathColumns.addAll(pctable.get(middleNamePath));
        String lastNamePath = "/*[name()='Customer']/*[name()='Name']/*[name()='LastName']";		pathColumns.addAll(pctable.get(lastNamePath));
        String dateOfBirthPath = "/*[name()='Customer']/*[name()='DateOfBirth']";					pathColumns.addAll(pctable.get(dateOfBirthPath));
        String genderPath = "/*[name()='Customer']/*[name()='Gender']";								pathColumns.addAll(pctable.get(genderPath));
        String nationalityPath = "/*[name()='Customer']/*[name()='Nationality']";					pathColumns.addAll(pctable.get(nationalityPath));
        String countryOfResidence = "/*[name()='Customer']/*[name()='CountryOfResidence']";			pathColumns.addAll(pctable.get(countryOfResidence));
        String languagePath = "/*[name()='Customer']/*[name()='Languages']/*[name()='Language']";	pathColumns.addAll(pctable.get(languagePath));
        String addressPath = "/*[name()='Customer']/*[name()='Addresses']";							pathColumns.addAll(pctable.get(addressPath));
        String emailaddressPath = "/*[name()='Customer']/*[name()='Addresses']/*[name()='EmailAddresses']/*[name()='Email']";	pathColumns.addAll(pctable.get(emailaddressPath));
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
        	finalResult.add(Bytes.toString(keyValue.getValue()) + "\r\n");
        }
        //�����������¼ʱ��
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
	 * @description ��ѯһ
	 * @author QiHaiyang
	 * @time 2016-1-16
	 * @param args
	 */
	public static void main(String[] args) {
		String tableName = "C2V-tpox";
		String P2Ctable = "P2C-tpox";
		Map<String, List<String>> pctable = null;
		Query3 q = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = TPoXSearch.P2C_Map;
			q = new Query3(tableName, pctable);
			for(int i = 0; i < 1; i++){
				q.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
