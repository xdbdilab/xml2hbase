package coding.xml2hbase.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseConf {
	

    static Configuration conf = null;
   
   
static {

    	 conf = HBaseConfiguration.create();
	    conf.set("hbase.master", "192.168.7.251");
		 conf.set("hbase.zookeeper.property.clientPort","2181");
		 conf.set("hbase.zookeeper.quorum","slave060,slave061,slave062,slave063,slave064,slave065,slave066,slave067,slave068,slave069,slave070,slave071,slave072,slave073,slave074,slave075,slave076,slave077,slave078,slave079,slave180,slave181,slave182,slave183,slave184,slave185,slave186,slave187,slave188,slave189,slave190,slave191,slave192,slave193,slave194,slave195,slave196,slave197,slave198,slave199,slave200,slave201,slave202,slave203,slave204,slave205,slave206,slave207,slave208,slave209,slave210");// ����Zookeeper��Ⱥ�ĵ�ַ�б�
  }
   
public static Configuration getConf()
{
	return conf;
}

}
