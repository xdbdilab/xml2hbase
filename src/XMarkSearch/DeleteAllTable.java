package XMarkSearch;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import cn.edu.xidian.repace.xml2hbase.hbase.HbaseConf;

public class DeleteAllTable {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Start delete operation");
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(HbaseConf.conf);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(String tableName : admin.getTableNames())
		{
			try {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}catch(TableNotEnabledException e)
			{
				admin.deleteTable(tableName);
				continue;
			}
		}
		System.out.println("End of delete operation");
	}

}
