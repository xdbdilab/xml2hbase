package cn.edu.xidian.repace.xml2hbase.hbase;

import java.io.IOException;


import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

//import cn.edu.xidian.repace.xml2hbase.MappingTable;

public class HbaseInsert {
	
	 public static void addRecord(String P2CtableName,Map<String, List<String>> path2Code,String family)
	 {
		// System.out.println("begin to insert data to path2Code");
		 if (!P2CtableName.equals("")) 
		   {
			 try {
                 //HTable table = new HTable(conf, P2CtableName);
				 HTable table = new HTable(HbaseConf.conf, P2CtableName); 
                 Put put = new Put(Bytes.toBytes(P2CtableName));
                 
                 String qualifier = "";
                 String Codes = "";
               	 
               	  for(Iterator i =path2Code.keySet().iterator(); i.hasNext();)
               	  {  
               		    Codes = "";
                    	qualifier = i.next().toString();
                    	List<String> list =path2Code.get(qualifier);	
                    
                    	for(int j = 0;j<list.size();j++)
                        {
                    	
                    		String s = list.get(j);	 	                    		
                    		Codes = Codes+ s+"#";	 	                    
                    	}
                    	
                    	  put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(Codes));
                  }
               	  
               	     table.put(put);	                      
                 
     //            System.out.println("insert " + P2CtableName + " into " + P2CtableName +" Success");
               }  catch (IOException e) { 
              	 System.out.println(e.getMessage() + "record insertion failed"); 
				     } 
			 
		   }
		 
	 }
	 
	 

	 public static void addRecord(String C2VtableName, String rowName, String family,Map<String, String> code2Value)
	 {
		// System.out.println("begin to insert data to code2value"); 
		 if (!C2VtableName.equals("")) 
		   { 
				if (!rowName.equals("")) 
				{ 
	               try {
	            	      HTable table = new HTable(HbaseConf.conf, C2VtableName);
	                      Put put = new Put(Bytes.toBytes(rowName));
	                      
	                      String qualifier = "";
	                      String Value = "";
	                    	  
	                     for (Iterator i = code2Value.keySet().iterator(); i.hasNext();)
	                     {	               
	                    	 qualifier = i.next().toString();
	                    	 Value =code2Value.get(qualifier);
	                    	 if(Value != null)                    
                  			 put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(Value));
	                    	 else 
                  			 put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(""));
                  	  	  
	                      }
	                      table.put(put);
//	                      System.out.println("insert " + rowName + " into " + C2VtableName +" Success");
	                    }  catch (IOException e) { 
	                   	 System.out.println(e.getMessage() + "record insertion failed"); 
						     } 
				}//if
				 else { 
					     System.out.println("rowName is empty"); 
					   } 
			}//if
		  else { 
			  System.out.println("C2VtableName is empty"); 
			}
		 
	 }
	 
	 
}
	 