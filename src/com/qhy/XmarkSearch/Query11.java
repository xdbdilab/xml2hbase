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

public class Query11 {

	private Map<String, List<String>> pctable;
	private String tableName;

	private final int MAX_THREAD_N = 30; // ����߳�����
	public ConcurrentHashMap<String, Double> personToIncome = new ConcurrentHashMap<String, Double>();
	public ConcurrentHashMap<String, Double> auctionIdToInitial = new ConcurrentHashMap<String, Double>();
	public ConcurrentHashMap<String, Integer> finalResult = new ConcurrentHashMap<String, Integer>();

	public Query11() {
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
	public Query11(String tableName, Map<String, List<String>> pctable) {
		this.pctable = pctable;
		this.tableName = tableName;
	}

	/**
	 * @throws IOException
	 * @description ��ѯ����
	 */
	public void query() throws IOException {
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
		List<String> personIdColumns = pctable.get(personId);
		List<String> personIncomeColumns = pctable.get(personIncome);
		// Map<String, Double> personToIncome = new HashMap<String, Double>();
		pathColumns.addAll(personIdColumns);
		pathColumns.addAll(personIncomeColumns);
		for (String column : pathColumns) {
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
		personIdColumns = null;
		personIncomeColumns = null;

		// ��ȡ��Ʒid����Ʒ�۸�
		rs.close();
		rs = null;
		filters.clear();
		filterList = null;
		pathColumns.clear();
		pathColumns.addAll(pctable.get(auctionId));
		pathColumns.addAll(pctable.get(auctionInitial));

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
		int threadn = 0; // ����ʹ�õ��߳�����
		int avgpersonn = 0; // ÿ���̴߳�������

		if (personCount / 10000 < MAX_THREAD_N) {
			threadn = personCount / 10000 + 1; // ��������1
		} else {
			threadn = MAX_THREAD_N;
		}
		avgpersonn = personCount / threadn + 1;
		System.out.println("Query11 : personCount " + personCount);
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

		for (Count count : jobs) {
			Thread thread = new Thread(count);
			thread.start();
		}

		// ���н�̽����ж�
		while (finalResult.keySet().size() != personCount) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			continue;
		}

		// ��������¼ʱ��
		long timeTestExcute = System.currentTimeMillis();
		WriteRecord.Record("Result11", finalResult.toString()); // ������
		try {
			Date dt = new Date();
			SimpleDateFormat matter1 = new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query11", matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @time 2016-1-12
	 * @author QiHaiyang ������
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		Query11 query = null;
		try {
			// pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable)
			pctable = XMarkSearch.P2C_Map;
			System.out.println(pctable.size());
			query = new Query11(tableName, pctable);
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
				for (String item : auctionIdToInitial.keySet()) {
					if (income > (auctionIdToInitial.get(item) * 5000)) {
						count = count + 1;
					}
				}
				finalResult.put(key, count);
			}
			nameToIncome.clear();
		}
	}
}
