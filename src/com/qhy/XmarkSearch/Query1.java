package com.qhy.XmarkSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import XMarkSearch.XMarkSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query1 {

	private Map<String, List<String>> pctable;
	private String tableName;

	public Query1() {
		pctable = null;
		tableName = null;
	}

	/**
	 * Constructor with parameters.
	 * 
	 * @param tableName
	 *            : the HBase table name .
	 * @param pctable
	 *            : the path to column mapping table.
	 */
	public Query1(String tableName, Map<String, List<String>> pctable) {
		this.pctable = pctable;
		this.tableName = tableName;
	}

	/**
	 * get the parent column of childCol
	 * 
	 * @param childCol
	 * @return parent column
	 */
	public static String getParentCol(String childCol) {
		int offset = childCol.lastIndexOf('.', childCol.length() - 1);
		int offset2 = childCol.lastIndexOf('.', offset - 1);
		return childCol.substring(0, offset2);
	}

	/**
	 * Run XQuery1
	 * 
	 * @throws IOException
	 */
	public void query() throws IOException {
		String xpath = "/site/people/person/@id";
		String xpath2 = "/site/people/person/name";
		String eqName = "person0";
		// Record the start time
		String finalResult = "";
		long timeTestStart = System.currentTimeMillis();

		List<String> columns = pctable.get(xpath);
		// ansList store the answers
		ArrayList<String> ansList = new ArrayList<String>();
		if (columns != null) {
			int i;
			List<Filter> filters = new ArrayList<Filter>();
			for (i = 0; i < columns.size(); ++i) {
				// New a SingleColumnValueFilter to filter on a qualifier
				SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("xmark"),
						Bytes.toBytes(columns.get(i)), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(eqName));
				// If the column is missing in a row, filter will skip
				filter.setFilterIfMissing(true);
				// Add the filter into filterList
				filters.add(filter);
			}
			// New a filterList and set the operator with MUST_PASS_ONE
			FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters); // ��һ��ͨ�����
			// Get the scan result of HBase table
			ResultScanner rscanner = HbaseReader.getRowsWithFilterList(tableName, filterlist, null, null);

			for (Result rs : rscanner) {
				for (i = 0; i < columns.size(); ++i) {
					if (Bytes.toString(rs.getValue(Bytes.toBytes("xmark"), Bytes.toBytes(columns.get(i))))
							.equals(eqName)) {
						String parentCol = getParentCol(columns.get(i));
						for (String col : pctable.get(xpath2)) {
							if (col.startsWith(parentCol)) {
								String value = Bytes.toString(rs.getValue(Bytes.toBytes("xmark"), Bytes.toBytes(col)));
								System.out.println("The result is : " + value);
								finalResult = value;
								ansList.add(value);
								break;
							}
						}
						// break;
					}
				}
			}
			// System.out.println("Outing");
			rscanner.close();
			// Record the end time
			long timeTestEnd = System.currentTimeMillis();
			// Print the running time.
			// System.out.println("The Query #1 Running time is:
			// "+(timeTestEnd-timeTestStart) + "ms");
			// ��������¼ʱ��
			long timeTestExcute = System.currentTimeMillis();
			WriteRecord.Record("Result1", finalResult); // ������
			try {
				Date dt = new Date();
				SimpleDateFormat matter1 = new SimpleDateFormat("yyyy-MM-dd");
				WriteRecord.Record("Query1", matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		Query1 q1 = null;
		try {
			
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			
			System.out.println("Got the Mapping Table");
			q1 = new Query1(tableName, pctable);
			for (int i = 0; i < 1; i++) {
				q1.query();
			}
			pctable = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
