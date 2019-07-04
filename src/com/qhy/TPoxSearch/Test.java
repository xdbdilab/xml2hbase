package com.qhy.TPoxSearch;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Result result = HbaseReader.getOneResult("C2V-tpox", Bytes.toBytes("order00000000"));
		List<KeyValue> keyValues = result.list();
		for(KeyValue keyValue : keyValues)
		{
			if(Bytes.toString(keyValue.getValue()).equals("103282"))
			{
				System.out.println(Bytes.toString(keyValue.getQualifier()));
			}
		}
	}

}
