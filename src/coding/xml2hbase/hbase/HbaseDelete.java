package coding.xml2hbase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;



public class HbaseDelete {

	public static void deleteTable(String tableName) throws Exception 
	{
		 if (!tableName.equals("")) { 
				try 
				  { 
					HBaseAdmin admin = new HBaseAdmin(HbaseConf.conf); 
					try {
						    if (admin.tableExists(tableName)) 
						   { 
						      admin.disableTable(tableName); //删除表前先对表禁用
						      admin.deleteTable(tableName); 
						      System.out.println("delete table" + tableName + "successful"); 
					        }//if 
						    else { 
						    	System.out.println( "Table"+tableName+"does not exist,so can not delete the table"); 
					              }//else
					   }//try
					catch (IOException e) { 
						System.out.println(e.getMessage() + "Delete table operation failure"); 
					   } 
				  }//try
				catch (MasterNotRunningException e) { 
					System.out.println(e.getMessage() + "Delete table operation failure"); 
				} 
				catch (ZooKeeperConnectionException e) { 	
					System.out.println(e.getMessage() + "Delete table operation failure"); 
				} 

			}//if
		  else { 
			    System.out.println("You choose to delete the entire table, this table does not exist, before you delete table please make sure the table you want to delete already exists"); 
			} //else
	}


	
	//删除一行数据,输入表名，行关键字

	 public static void delRecord(String tableName, String rowKey) throws IOException
	 {
		  if (!tableName.equals("")) 
		      { 
				if (!rowKey.equals("")) 
				  { 
					try { 
	                     HTable table = new HTable(HbaseConf.conf, tableName);
	                     List list = new ArrayList();
	                     Delete del = new Delete(rowKey.getBytes());
	                      list.add(del);
	                      table.delete(list);
	                      System.out.println("delete record" + rowKey + " success");
					    }//try
					 catch (IOException e) { 
						 System.out.println(e.getMessage() + "delete record failed"); 
						 }
				  }//if
				 else { 
					System.out.println("You choose to delete the entire row record, the current rowName is empty, before you delete record  please enter the rowKey"); 
				   } 
			  }//if
		    else { 
		    	System.out.println("this table does not exist, before you delete record  please make sure the table already exists"); 
			   } 
	 }		
	 
	 

}
