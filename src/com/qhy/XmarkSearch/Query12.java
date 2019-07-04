package com.qhy.XmarkSearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

public class Query12 {

	private Map<String, List<String>> pctable;
	private String tableName;

	private final int MAX_THREAD_N = 30; // ����߳�����
	public ConcurrentHashMap<String, Double> personToIncome = new ConcurrentHashMap<String, Double>();
	public ConcurrentHashMap<String, Double> auctionIdToInitial = new ConcurrentHashMap<String, Double>();
	public ConcurrentHashMap<String, Integer> finalResult = new ConcurrentHashMap<String, Integer>();

	public static int personOverAve = 0;
	public static double aveIncome = 0.0;
	public static double allIncome = 0.0;

	public Query12() {
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
	public Query12(String tableName, Map<String, List<String>> pctable) {
		this.pctable = pctable;
		this.tableName = tableName;
	}

	/**
	 * @throws IOException
	 * @description ��ѯ����
	 */
	public void query() throws IOException {

		personOverAve = 0;
		aveIncome = 0.0;
		allIncome = 0.0;
		finalResult.clear();
		personToIncome.clear();
		auctionIdToInitial.clear();

		long timeTestStart = System.currentTimeMillis();

		List<Count> jobs = new ArrayList<Count>();
		String personId = "/site/people/person/@id";
		String personIncome = "/site/people/person/profile/@income";

		String auctionId = "/site/open_auctions/open_auction/@id";
		String auctionInitial = "/site/open_auctions/open_auction/initial";

		List<String> pathColumns = new ArrayList<String>(); // ����xpath���Ե����ĵ����Ƶı���
		List<Filter> filters = new ArrayList<Filter>();
		// ��ȡ�������Ϣ
		List<String> personIdColumns = XMarkSearch.P2C_Map.get(personId);
		System.out.println(XMarkSearch.P2C_Map.size());
		System.out.println( "personIdColumns   "    +  personIdColumns.size());
		List<String> personIncomeColumns = XMarkSearch.P2C_Map.get(personIncome);
		// Map<String, Double> personToIncome = new HashMap<String, Double>();
		pathColumns.addAll(personIdColumns);
		pathColumns.addAll(personIncomeColumns);
		for (String column : pathColumns) {
			System.out.println(column);
			Filter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(column)));
			filters.add(filter);
		}
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
		ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
		int personCount = 0; // ��¼����
		for (Result result : rs) {
			List<KeyValue> keyValues = result.list();
			for (int i = 0; i < keyValues.size();) {
				KeyValue keyValue = keyValues.get(i);
				String qualifier = Bytes.toString(keyValue.getQualifier());
				String value = Bytes.toString(keyValue.getValue());

				String nextQualifier = "";
				try {
					nextQualifier = Bytes.toString(keyValues.get(i + 1).getQualifier());
				} catch (ArrayIndexOutOfBoundsException e) {
					// ������ܻ����Խ������
					if (personIdColumns.contains(qualifier)) {
						personCount = personCount + 1;
						personToIncome.put(value, 0.0);
					}
					break;
				}
				if (personIdColumns.contains(qualifier))
				// �����id
				{
					personCount = personCount + 1; // ����+1
					if (personIncomeColumns.contains(nextQualifier))
					// ��һ�������income����Ϣ
					{
						personToIncome.put(value, Double.parseDouble(Bytes.toString(keyValues.get(i + 1).getValue())));
						allIncome = allIncome + Double.parseDouble(Bytes.toString(keyValues.get(i + 1).getValue()));
						i = i + 2; // i����Ҫ����2
					} else {
						// �����������Ϣ
						personToIncome.put(value, 0.0);
						i = i + 1; // �ƶ�����һ��
						continue;
					}
				} else {
					// �����income
					if (personIdColumns.contains(nextQualifier))
					// ��һ����id����Ϣ
					{
						personCount = personCount + 1; // ����+1
						personToIncome.put(Bytes.toString(keyValues.get(i + 1).getValue()), Double.parseDouble(value));
						allIncome = allIncome + Double.parseDouble(value);
						i = i + 2;
					} else {
						// ����������Զ���ᴥ��������
						System.out.println("�²����Ŷ");
						i = i + 1;
						continue;
					}
				}
			}
		}
//		personIdColumns.clear();
//		personIncomeColumns.clear();

		// ��ȡ��Ʒid����Ʒ�۸�
		rs.close();
		rs = null;
		filters.clear();
		filterList = null;
		pathColumns.clear();
		pathColumns.addAll(XMarkSearch.P2C_Map.get(auctionId));
		pathColumns.addAll(XMarkSearch.P2C_Map.get(auctionInitial));

		for (String column : pathColumns) {
			Filter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(column)));
			filters.add(filter);
		}
		filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
		rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
		for (Result result : rs) {
			List<KeyValue> keyValues = result.list();
			for (int i = 0; i < keyValues.size(); i = i + 2) {
				auctionIdToInitial.put(Bytes.toString(keyValues.get(i + 1).getValue()),
						Double.parseDouble(Bytes.toString(keyValues.get(i).getValue())));
			}
		}
		rs.close();

		// ѡ����ʱ���߳�����
		aveIncome = allIncome / personCount;
		int threadn = 0; // ����ʹ�õ��߳�����
		int avgpersonn = 0; // ÿ���̴߳�������

		if (personCount / 10000 < MAX_THREAD_N) {
			threadn = personCount / 10000 + 1; // ��������1
		} else {
			threadn = MAX_THREAD_N;
		}
		System.out.println("  personCount  " + personCount);
		avgpersonn = personCount / threadn + 1;
		personOverAve = threadn * (-1);

		HashMap<String, Double> excutMap = new HashMap<String, Double>(); // ÿ�ν��д����������Ϣ

		int currentThreadNum = 0;
		int excuten = 0; // ��¼������������Ϣ���������ڷ���������С��
		for (String person : personToIncome.keySet()) {
			excutMap.put(person, personToIncome.get(person));
			excuten = excuten + 1; // ��������+1
			if ((excuten % avgpersonn) == 0 || excuten >= personCount) {
				Count temp = new Count(excutMap, currentThreadNum);
				jobs.add(temp);
				temp = null;
				// ����Map
				currentThreadNum++;
				excutMap = null;
				excutMap = new HashMap<String, Double>();
			}
		}

		System.out.println(" " + personOverAve);
		for (Count count : jobs) {
			Thread thread = new Thread(count);
			thread.start();
		}

		// ���н�̽����ж�
		while (personOverAve != 0) {
			try {
				Thread.sleep(1000);
				// System.out.println(finalResult.keySet().size() + " " +
				// personOverAve);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			continue;
		}

		// ��������¼ʱ��
		long timeTestExcute = System.currentTimeMillis();
		WriteRecord.Record("Result12", finalResult.toString()); // ������
		try {
			Date dt = new Date();
			SimpleDateFormat matter1 = new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query12", matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @author QiHaiyang
	 * @time 2016-1-12
	 */
	public static void main(String[] args) {
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		Query12 query = null;
		try {
			// pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable,
			// P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query12(tableName, pctable);
			for (int i = 0; i < 1; i++) {
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}

	// �߳�����
	public class Count implements Runnable {

		private HashMap<String, Double> nameToIncome = new HashMap<String, Double>();
		private int n;

		public Count(HashMap<String, Double> NameToIncome, int n) {
			// TODO Auto-generated constructor stub
			this.nameToIncome = NameToIncome;
			this.n = n;
		}

		@Override
		public void run() {
			for (String key : nameToIncome.keySet()) {
				int count = 0;
				double income = nameToIncome.get(key);
				if (income > aveIncome) {
					for (String item : auctionIdToInitial.keySet()) {
						if (income > (auctionIdToInitial.get(item) * 5000)) {
							count = count + 1;
						}
					}
					finalResult.put(key, count);
					// personOverAve = personOverAve + 1;
				}
			}
			nameToIncome.clear();
			personOverAve = personOverAve + 1;
		}
	}
}
