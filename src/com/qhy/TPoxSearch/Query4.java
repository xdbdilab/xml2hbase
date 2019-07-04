package com.qhy.TPoxSearch;

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

public class Query4 {

	private Map<String, List<String>> pctable;
	private String tableName;
	
	private String sector = "Energy";
	
	public Query4() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query4(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	public void query() throws IOException
	{
		long timeTestStart=System.currentTimeMillis();
		String sectorPath = "/*[name()='Security']/*[name()='SecurityInformation']/*[name()='StockInformation']/*[name()='Sector']";
		List<String> sectorColumns = pctable.get(sectorPath);
		String PEPath = "/*[name()='Security']/*[name()='PE']";
		List<String> PEColumns = pctable.get(PEPath);
		String yieldPath = "/*[name()='Security']/*[name()='Yield']";
		List<String> yieldColumns = pctable.get(yieldPath);
		String symbolPath = "/*[name()='Security']/*[name()='Symbol']";
		List<String> symbolColumns = pctable.get(symbolPath);
		String namePath = "/*[name()='Security']/*[name()='Name']";
		List<String> nameColumns = pctable.get(namePath);
		String typePath = "/*[name()='Security']/*[name()='SecurityType']";
		List<String> typeColumns = pctable.get(typePath);
		
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        List<Filter> filters = new ArrayList<Filter>();
        List<Security> finalResult = new ArrayList<Security>();
        //�Ȼ�ȡsector������������
        pathColumns.addAll(sectorColumns);
        for(String column : pathColumns)
        {
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					  Bytes.toBytes("tpox"),
					  Bytes.toBytes(column),
					  CompareFilter.CompareOp.EQUAL,
					  Bytes.toBytes(sector)
					  );
			//If the column is missing in a row, filter will skip
			filter.setFilterIfMissing(true);
			filters.add(filter);
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE , filters);
        ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList,null,null);
        List<byte[]> rows = new ArrayList<byte[]>();	//�洢�ҵ�����
        for(Result result : rs)
        {
        	rows.add(result.getRow());
        }
        //��ȡ�Ա�����	
        rs.close();
        filters.clear();
        //�����Ҫ�ĸ���ѡ��
        pathColumns.addAll(PEColumns);	
        pathColumns.addAll(yieldColumns);
        pathColumns.addAll(symbolColumns);
        pathColumns.addAll(nameColumns);
        pathColumns.addAll(typeColumns);
        
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(column)));
        	filters.add(filter);
        }
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE , filters);
        List<Get> gets = new ArrayList<Get>();
        for(byte[] row : rows)
        {
        	Get get = new Get(row);
        	get.setFilter(filterList);
        	gets.add(get);
        }
        Result[] results = HbaseReader.getByGetList(tableName, gets);
        for(Result result : results)
        {
        	int number = 0 ;							//��¼������������Ŀ�����Ϊ0��������
        	Security security = new Security();
        	List<KeyValue> keyValues = result.list();
        	for(KeyValue keyValue : keyValues)
        	{
        		String qualifier = Bytes.toString(keyValue.getQualifier());
        		if(PEColumns.contains(qualifier))
        		{
        			Double value = Double.parseDouble(Bytes.toString(keyValue.getValue()));
        			if( (value >= 30) && (value < 35)){
        				number++;
        			}
        			security.setPE(value + "");;
        		}else if (yieldColumns.contains(qualifier)) {
        			Double value = Double.parseDouble(Bytes.toString(keyValue.getValue()));
        			if(value > 4.5){
        				number++;
        			}
        			security.setYield(value + "");
				}else if(nameColumns.contains(qualifier)){
					String value = Bytes.toString(keyValue.getValue());
					security.setName(value);
				}else if (typeColumns.contains(qualifier)) {
					String value = Bytes.toString(keyValue.getValue());
					security.setSecurityType(value);;
				}else if (sectorColumns.contains(qualifier)) {
					String value = Bytes.toString(keyValue.getValue());
					security.setSector(value);
				}else {
					String value = Bytes.toString(keyValue.getValue());
					security.setSymbol(value);
				}
        	}
        	if (number == 2) {
				finalResult.add(security);
			}
        }
        //�����������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result4", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query4",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
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
		Query4 q = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = TPoXSearch.P2C_Map;
			q = new Query4(tableName, pctable);
			for(int i = 0; i < 1; i++){
				q.query();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	class Security{
		String Symbol = "";
		String Name = "";
		String SecurityType = "";
		String Sector = "";
		String PE = "";
		String Yield = "";
		
		void setSymbol(String Symbol)
		{
			this.Symbol = Symbol;
		}
		String getSymbol()
		{
			return this.Symbol;
		}
		void setName(String Name)
		{
			this.Name = Name;
		}
		String getName()
		{
			return this.Name;
		}
		void setSecurityType(String SecurityType)
		{
			this.SecurityType = SecurityType;
		}
		String getSecurityType()
		{
			return this.SecurityType;
		}
		void setSector(String Sector)
		{
			this.Sector = Sector;
		}
		String getSector()
		{
			return this.Sector;
		}
		void setPE(String PE)
		{
			this.PE = PE;
		}
		String getPE()
		{
			return this.PE;
		}
		void setYield(String Yield)
		{
			this.Yield = Yield;
		}
		String getYield()
		{
			return this.getYield();
		}
		@Override
		public String toString()
		{
			String result = "<Security>\r\n" +
							"    " + "Symbol=" + Symbol +"\r\n" +
							"    " + "Name="   + Name  + "\r\n" +
							"    " + "SecurityType=" + SecurityType + "\r\n" +
							"    " + "Sector=" + sector + "\r\n" +
							"    " + "PE=" + PE + "\r\n" +
							"    " + "Yield=" + Yield + "\r\n" +
							"</Security>";
			return result;
		}
	}
}
