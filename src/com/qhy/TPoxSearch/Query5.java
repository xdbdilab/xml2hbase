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

public class Query5 {

	private Map<String, List<String>> pctable;
	private String tableName;
	
	private String id = "1002";
	
	public Query5() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query5(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	public void query() throws IOException
	{
		long timeTestStart=System.currentTimeMillis();
		String idPath = "/*[name()='Customer']/@id";
		List<String> idColumns = pctable.get(idPath);
		String titlePath = "/*[name()='Customer']/*[name()='Name']/*[name()='Title']";
		List<String> titleColumns = pctable.get(titlePath);
		String firstNamePath = "/*[name()='Customer']/*[name()='Name']/*[name()='FirstName']";
		List<String> firstNameColumns = pctable.get(firstNamePath);
		String middleNamePath = "/*[name()='Customer']/*[name()='Name']/*[name()='MiddleName']";
		List<String> middleNameColumns = pctable.get(middleNamePath);
		String lastNamePath = "/*[name()='Customer']/*[name()='Name']/*[name()='LastName']";
		List<String> lastNameColumns = pctable.get(lastNamePath);
		String accountIDPath = "/*[name()='Customer']/*[name()='Accounts']/*[name()='Account']/@id";
		List<String> accountIDColumns = pctable.get(accountIDPath);
		String balancePath = "/*[name()='Customer']/*[name()='Accounts']/*[name()='Account']/*[name()='Balance']/*[name()='OnlineActualBal']";
		List<String> balanceColumns = pctable.get(balancePath);
		String securityName = "/*[name()='Customer']/*[name()='Accounts']/*[name()='Account']/*[name()='Holdings']/*[name()='Position']/*[name()='Name']";
		List<String> securityNameColumns = pctable.get(securityName); 
		
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        List<Filter> filters = new ArrayList<Filter>();
        
        pathColumns.addAll(idColumns);
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
        byte[] row = null;
        for(Result result : rs)
        {
        	row = result.getRow();
        	System.out.println(Bytes.toString(row));
        }
        filters.clear();
        rs.close();
        pathColumns.addAll(titleColumns);	pathColumns.addAll(firstNameColumns);	pathColumns.addAll(middleNameColumns);
        pathColumns.addAll(lastNameColumns);	pathColumns.addAll(accountIDColumns);	pathColumns.addAll(balanceColumns);
        pathColumns.addAll(securityNameColumns);
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(column)));
        	filters.add(filter);
        }
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE , filters);
        Result result = HbaseReader.getOneResultWithFilters(tableName, row, filterList);
        List<KeyValue> keyValues = result.list();
        Custacc custacc = new Custacc();
        for(KeyValue keyValue : keyValues)
        {
        	String qualifier = Bytes.toString(keyValue.getQualifier());
        	String value = Bytes.toString(keyValue.getValue());
        	if(idColumns.contains(qualifier))
        	{
        		custacc.setId(value);
        	}else if (titleColumns.contains(qualifier)) {
				custacc.setTitle(value);
			}else if (firstNameColumns.contains(qualifier)) {
				custacc.setFirstName(value);
			}else if (middleNameColumns.contains(qualifier)) {
				custacc.setMiddleName(value);
			}else if (lastNameColumns.contains(qualifier)) {
				custacc.setLastName(value);
			}else if (accountIDColumns.contains(qualifier)) {
				custacc.setAccountID(value);
			}else if (balanceColumns.contains(qualifier)) {
				custacc.setBalance(value);
			}else{
				custacc.addNames(value);
			}
        }
        
        //�����������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result5", custacc.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query5",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
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
		Query5 q = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = TPoXSearch.P2C_Map;
			q = new Query5(tableName, pctable);
			for(int i = 0; i < 1; i++){
				q.query();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	class Custacc{
		String id = "";
		String title = "";
		String firstName = "";
		String middleName = "";
		String lastName = "";
		String accountID = "";
		String balance = "";
		List<String> securityNames = new ArrayList<String>();
		void setId(String id)
		{
			this.id = id;
		}
		void setTitle(String title)
		{
			this.title = title;
		}
		void setFirstName(String firstName)
		{
			this.firstName = firstName;
		}
		void setMiddleName(String middleName)
		{
			this.middleName = middleName;
		}
		void setLastName(String lastName)
		{
			this.lastName = lastName;
		}
		void setAccountID(String accountID)
		{
			this.accountID = accountID;
		}
		void setBalance(String balance)
		{
			this.balance = balance;
		}
		void addNames(String name)
		{
			this.securityNames.add(name);
		}
		@Override
		public String toString()
		{
			String result = "";
			result = "<Customer id=" + id + ">" + "\r\n" +
					 "    " + "<Name>" + "\r\n" +
					 "    " + "    "+"<Tittle>" + title + "</Tittle>" + "\r\n" +
					 "    " + "    "+"<FirstName>" + firstName + "</FirstName>" + "\r\n" +
					 "    " + "    "+"<MiddleName>"+ middleName+ "</MiddleName>" + "\r\n" +
					 "    " + "</Name>" + "\r\n" +
					 "    " + "<Account ID=" + accountID + "    BALANCE=" + balance +">" + "\r\n" +
					 "    " + "    "+ "<Security>" + "\r\n" +
					 "    " + "    "+ "    " + "<Names>" + securityNames.toString() + "</Names>" + "\r\n" +
					 "    " + "    "+ "</Security>" + "\r\n" +
					 "    " + "</Account>" + "\r\n" +
					 "</Customer>";
			return result;
		}
	}
}
