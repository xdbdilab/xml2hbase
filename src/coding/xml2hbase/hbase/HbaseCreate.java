package coding.xml2hbase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseCreate {
	 public static void createTable(String tableName,String familys) throws Exception {  
//	        System.out.println("start create table ......");  
	        try {  
	           // HBaseAdmin hBaseAdmin = new HBaseAdmin(conf); 
	        	HBaseAdmin hBaseAdmin = new HBaseAdmin(HbaseConf.conf);
	            if (hBaseAdmin.tableExists(tableName)) {
	            	System.out.println("table"+ tableName + "already exists"); 
	            }  
	            else
	            {
	            	HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
	            	tableDescriptor.addFamily(new HColumnDescriptor(familys));  
	            	hBaseAdmin.createTable(tableDescriptor);  
	            }
	        } catch (MasterNotRunningException e) {  
	            e.printStackTrace();  
	        } catch (ZooKeeperConnectionException e) {  
	            e.printStackTrace();  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
//	        System.out.println("table"+ tableName + "create successful"); 
	         
	    }  
}
	    