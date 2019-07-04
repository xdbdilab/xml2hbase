package cn.edu.xidian.repace.xml2hbase.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseConf {
	
	private static HBaseConfiguration hbaseConfig=null;
    public static Configuration conf = null;//modded the conf to public by kanchuanqi
   
   
    
    static {

        	 conf = HBaseConfiguration.create();
    	    conf.set("hbase.master", "192.168.7.251");
    		 conf.set("hbase.zookeeper.property.clientPort","2181");
    	    conf.set("hbase.zookeeper.quorum","slave060,slave061,slave062,slave063,slave064,slave065,slave066,slave067,slave068,slave069,slave070,slave071,slave072,slave073,slave074,slave075,slave076,slave077,slave078,slave079,slave180,slave181,slave182,slave183,slave184,slave185,slave186,slave187,slave188,slave189,slave190,slave191,slave192,slave193,slave194,slave195,slave196,slave197,slave198,slave199,slave200,slave201,slave202,slave203,slave204,slave205,slave206,slave207,slave208,slave209,slave210");// ����Zookeeper��Ⱥ�ĵ�ַ�б�
    }

public static void create(String master,String port,String quorum){
	 conf = HBaseConfiguration.create();
 	 conf.set("hbase.master", master);
 	 conf.set("hbase.zookeeper.property.clientPort",port);
 	 conf.set("hbase.zookeeper.quorum",quorum);//192.168.1.7,
}
   
public static Configuration getConf()
{
	return conf;
}
public static String getVersion(){
	return conf.get("hbase.defaults.for.version");
}

public static String getRootDir(){
	return conf.get("hbase.rootdir");
}
public static String getHadoopVersion(){
	return conf.get("hadoop.defaults.for.version");
}
public static String getTmpDir(){
	return conf.get("hbase.tmp.dir");
}

public static String getId(){
	return conf.get("hbase.cluster.id");
}
public static String getZook(){
	return conf.get("hbase.zookeeper.quorum");
}
public static String getDis(){
	return conf.get("hbase.cluster.distributed");
}

}
