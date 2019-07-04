package coding.xml2hbase.hbase;





import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import cn.edu.xidian.repace.xml2hbase.filter.PagePoint;


public class HbaseReader {
		
	
    public static void getAllRecord (String tableName) 
    {
      
    	if (!tableName.equals(""))
        {
             try{
                  HTable table = new HTable(HbaseConf.conf, tableName);
                  Scan s = new Scan();
                  ResultScanner rs = table.getScanner(s);
                  for(Result r:rs)
                  {
                        for(KeyValue kv : r.raw())
                        {
                           System.out.print(new String(kv.getRow()) + " ");
                           System.out.print(new String(kv.getFamily()) + ":");
                           System.out.print(new String(kv.getQualifier()) + " ");
                           System.out.print(kv.getTimestamp() + " ");
                           System.out.println(new String(kv.getValue()));
                        }//for
                  }//for
                }//try 
             catch (IOException e)
             {
           	  System.out.println(e.getMessage() + "scan failed"); 
              }
        }
        else
        {
       	 System.out.println(" the tablename is null,please enter the ensure tablename"); 
        
        }
     
    }
    
    //閿熶粙鐪嬮敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熷彨纭锋嫹褰�閿熸枻鎷烽敓鏂ゆ嫹HbaseInfor閿熸枻鎷烽敓鏂ゆ嫹閿熷彨鍏崇》鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓浠嬶紝閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓浠嬶紝閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鐨嗙�?鎷峰�閿熸枻鎷烽敓浠嬶紱閿熻妭纰夋嫹閿熺煫璇ョ尨鎷烽敓鏂ゆ嫹鍓嶉敓鏂ゆ嫹閿熸枻鎷峰閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷�
    

    public static void getAllRecord (HbaseInfor object,List<String> rowkey) 
    {
       if (!object.tableName.equals(""))
       {
            try{
                 HTable table = new HTable(HbaseConf.conf, object.tableName);
                 Scan s = new Scan();
                 ResultScanner rs = table.getScanner(s);
                 for(Result res:rs)
                 {
                 	 
                 	 object.rowKey=new String(res.getRow());
              	     rowkey.add(object.rowKey);
                       for(KeyValue kv : res.raw())
                       {
                     	   object.qualifier=new String(kv.getFamily());
                    	   object.value=new String(kv.getValue()); 
                     	 //qualifier.add( object.getQualifier());
                     	 //value.add(object.getValue);
                        }//for
                 }//for
               }//try 
            catch (IOException e)
            {
          	  System.out.println(e.getMessage() + "scan failed"); 
             }
       }
       else
       {
      	 System.out.println(" this table does not exist,please make sure the table you want to scan"); 
       
       }
    }
    
      
    //Get all rowkey of a table
    public static ArrayList<String> getAllRowKeys(String tableName){
    	ArrayList<String> ans = new ArrayList<String>();
    	if (!tableName.equals(""))
        {
             try{
                  HTable table = new HTable(HbaseConf.conf, tableName);
                  Scan s = new Scan();
                  s.setFilter(new FirstKeyOnlyFilter());
                  ResultScanner rs = table.getScanner(s);
                  for(Result r:rs)
                  {
                        for(KeyValue kv : r.raw())
                        {
                           ans.add(Bytes.toString(kv.getRow()));
                         }//for
                  }//for
                }//try 
             catch (IOException e)
             {
           	  System.out.println(e.getMessage() + "scan failed"); 
              }
        }
        else
        {
       	 System.out.println(" the tablename is null,please enter the ensure tablename"); 
        
        }
    	return ans;
    }
    
    //閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熷彨鍏崇�?鎷烽敓鏂ゆ嫹,閿熸枻鎷峰彇涓�敓鍙�?鎷峰�?
	 
	   public static void getOneRecord (String tableName, String rowKey) throws IOException
	   {
		  if(!tableName.equals(""))
		  {
			  if(!rowKey.equals(""))
			  { 
				  try
				  {
					 // int count=0;
					  HTable table = new HTable(HbaseConf.conf, tableName);
					  Get get = new Get(rowKey.getBytes());
					  Result rs = table.get(get);
					  for(KeyValue kv : rs.raw())
					  {
						  System.out.print(new String(kv.getRow()) + " " );
						  System.out.print(new String(kv.getFamily()) + ":" );
						  System.out.print(new String(kv.getQualifier())+":");						  
						  System.out.print(kv.getTimestamp() + " " );
						  System.out.println(new String(kv.getValue()));
						 // count++;
						  
					  }
					  //System.out.println("count="+count);
				  }
				  catch (IOException e) { 
				// TODO Auto-generated catch block 
					  System.out.println(e.getMessage() + "scan failed!"); 
				  } 
			  }//if
			  else{
				  System.out.println("the rowkey is null,please enter ensure rowkey!");
			  }
		  }//if
		  else{
			  System.out.println("the tablename is null,please enter ensure tablename!");
		  }
	    

	   }
	   
	 //閿熸枻鎷烽敓鎻紮鎷峰睉閿熼摪纭锋嫹閿熸枻鎷烽敓鏂ゆ嫹閿熸彮浼欐嫹閿熺纯baseInfor閿熸枻鎷烽敓鏂ゆ嫹閿熻妭纰夋嫹閿熸枻鎷烽敓鏂ゆ嫹鏃堕敓鏂ゆ嫹閿熸枻鎷峰閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹娉勪付閿熸枻鎷烽敓锟�?
	   public static void getOneRecord (HbaseInfor object,List<String> qualifierlist,List<String> valuelist) throws IOException
	   {
	  	  
	      try
	        {
	      	 object.column="";
	      	 object.qualifier="";
	      	 object.value=""; 	   
	           HTable table = new HTable(HbaseConf.conf, object.tableName);
	           Get get = new Get(Bytes.toBytes(object.rowKey));
	           Result rs = table.get(get);
	              
	           for(KeyValue kv : rs.raw())
	           {
	          	   object.qualifier=new String(kv.getQualifier());
	               object.column=new String(kv.getFamily());
	               object.rowKey=new String(kv.getRow());
	               object.value=new String(kv.getValue()); 
	               qualifierlist.add(object.qualifier);
	               valuelist.add(object.value);
	           }//for           
	           
	         }
	      catch (IOException e) { 
	  			// TODO Auto-generated catch block 
	   	   System.out.println(e.getMessage() + "scan failed!"); 
	  		} 
	    

	   }
	   
	   
	    //閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹娉勪付閿熸枻鎷烽儜閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹鍙栨煇涓�敓鏂ゆ嫹鏌愪竴閿熸枻鎷烽敓鍙鎷烽敓鏂ゆ嫹閿燂拷
	    

		public static String  getoneQualifier(String tableName,String rowKey,String column,String qualifier) throws IOException
	     { 
	    	 String value = null;
	    	 if (!tableName.equals(""))
	         {
	   		  if (!rowKey.equals(""))
	   		  	{ 
	   			  if (!column.equals(""))
	   			  { 
	   				  if(!qualifier.equals(""))
	   				  {
	   					  try
	   					  {
	   						  HTable table=new HTable(HbaseConf.conf,tableName);  
	   						  Get get = new Get(Bytes.toBytes(rowKey)); 
	   						  get.addColumn(Bytes.toBytes(column), Bytes.toBytes(qualifier));
	   						  Result r=table.get(get);
	   						  byte[] val=r.getValue(Bytes.toBytes(column), Bytes.toBytes(qualifier));				  
	   						  value = Bytes.toString(val);
	   						 // System.out.println(rowKey+"  "+"value="+value);
	   						  //String temp[]=value.split("#");
	   						 // System.out.println("size="+temp.length);
	   						 
	   					  }//try
	   					catch (IOException e) 
	     				  { 
	     				  // TODO Auto-generated catch block 
	     				  System.out.println(e.getMessage() + "scan failed"); 
	     				  }
	   				
	   				  }//if
	   				  else
	   				  {
	   					System.out.println(" the qualifier is null,please enter the ensure qualifier ");
	   				  }
	   			   }//if
	   			  else
	   			  {
					System.out.println(" the family is null,please enter the ensure family");
	   			  }
	   		  	}//if
	   		 else
				{
				System.out.println(" the rowkey is null,please enter the ensure rowkey ");
				 }
	         }//if
	    	 else
	    	 {
	    	System.out.println(" the table is null,please enter the ensure table ");
	    	 }
	    	 return value;
	     }
		
		public static ResultScanner  getRowWithEqualFilter(String tableName,String rowKey, FilterList filters) throws IOException
	     { 		
			System.out.println("dataBase="+tableName);
			System.out.println("rowKey="+rowKey);
	    	 String value = null;
//	    	 Result rs = null;
	    	 ResultScanner scanner=null;
	    	 if (!tableName.equals(""))
	         {
	   		  if (!rowKey.equals(""))
	   		  	{ 
				  try
				  {
					  HTable table=new HTable(HbaseConf.conf,tableName);  
					 Scan scan =new Scan();
					 scan.setFilter(filters);

					scanner=table.getScanner(scan);
					/*
					for(Result result:scanner)
					{
						for(KeyValue kv:result.raw())
						{
							System.out.println("KV:"+kv+",Value:"+Bytes.toString(kv.getValue()));
							//System.out.println("KV:"+Bytes.toString(kv.getQualifier())+",Value:"+Bytes.toString(kv.getValue()));
						}
					}
					scanner.close();
*/
					 
				  }//try
				catch (IOException e) 
 				  { 
 				  // TODO Auto-generated catch block 
 				  System.out.println(e.getMessage() + "scan failed"); 
 				  }

	   		  	}//if
	   		 else
				{
				System.out.println(" the rowkey is null,please enter the ensure rowkey ");
				 }
	         }//if
	    	 else
	    	 {
	    	System.out.println(" the table is null,please enter the ensure table ");
	    	 }
	    	// return rs;
	    	 return scanner;
	     }
		
		//閿熸枻鎷烽敓鏂ゆ嫹HbaseInfor閿熸枻鎷烽敓鏂ゆ嫹閿熺殕纭锋嫹鍊奸敓鏂ゆ嫹閿熶粙锛涢敓鑺傜鎷烽敓鐭尨鎷烽敓鏂ゆ嫹涔嬪墠閿熸枻鎷峰閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鍙叧纭锋嫹閿熻锝忔嫹閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓楗衡�?��嬩紮鎷烽敓渚ラ獘浼欐嫹閿熸枻鎷锋閿熸枻鎷烽敓鏂ゆ嫹
		
		public static void  getoneQualifier(HbaseInfor object,List<String> value) throws IOException
		 { 
			 
			 if (!object.tableName.equals(""))
		     {
			  if (!object.rowKey.equals(""))
			  	{ 
				  if (!object.column.equals(""))
				  { 
					  if(!object.qualifier.equals(""))
					  {
						  try
						  {    
							  object.value="";
							  HTable table=new HTable(HbaseConf.conf,object.tableName);  
							  Get get = new Get(Bytes.toBytes(object.rowKey)); 
							  get.addColumn(Bytes.toBytes(object.column), Bytes.toBytes(object.qualifier));
							  Result r=table.get(get);
							  byte[] val=r.getValue(Bytes.toBytes(object.column), Bytes.toBytes(object.qualifier));
							  object.value=Bytes.toString(val);
							  value.add(object.value);
							 // System.out.println("rowKey: "+new String(r.getRow())+"   qualifier閿熸枻鎷�?object.qualifier+"    value: "+object.value);
							 
						  }//try
						catch (IOException e) 
		 				  { 
		 				  // TODO Auto-generated catch block 
		 				  System.out.println(e.getMessage() + "scan failed"); 
		 				  }
					
					  }//if
					  else
					  {
						System.out.println(" the qualifier is null,please enter the ensure qualifier ");
					  }
				   }//if
				  else
				  {
					System.out.println(" the family is null,please enter the ensure family");
				  }
			  	}//if
			 else
				{
				System.out.println(" the rowkey is null,please enter the ensure rowkey ");
				 }
		     }//if
			 else
			 {
			System.out.println(" the table is null,please enter the ensure table ");
			 }
			 
		 }
		
		
		
		
		public static ResultScanner  getRowsWithFilterList(String tableName, FilterList filters,String startRow,String stopRow) throws IOException
		{ 
			//String value = null;
			ResultScanner rs = null;
			if (!tableName.equals(""))
		    {
				try
				{
					HTable table=new HTable(HbaseConf.conf,tableName);  
					Scan scan = new Scan(); 
					if(startRow!=null){
						scan.setStartRow(Bytes.toBytes(startRow));
					}
					if(stopRow!=null){
						scan.setStopRow(Bytes.toBytes(stopRow));
					}
					scan.setFilter(filters);
					rs = table.getScanner(scan);			 
				}catch (IOException e) 
				{ 
					System.out.println(e.getMessage() + "scan failed"); 
				}
		    }//if
			else
			{
				System.out.println(" the table is null,please enter the ensure table ");
			}
			return rs;
		}
		
		//This function is added by kanchuanqi 2013.3.19 for getValues()
		public static ResultScanner getRowKeys(String tableName,String startRow,String stopRow){
			ResultScanner rs = null;
			if (!tableName.equals(""))
		    {
				try
				{
					HTable table=new HTable(HbaseConf.conf,tableName);  
					Scan scan = new Scan(); 
					if(startRow!=null){
						scan.setStartRow(Bytes.toBytes(startRow));
					}
					if(stopRow!=null){
						scan.setStopRow(Bytes.toBytes(stopRow));
					}
					//scan.setFilter(filters);
					rs = table.getScanner(scan);			 
				}catch (IOException e) 
				{ 
					System.out.println(e.getMessage() + "scan failed"); 
				}
		    }//if
			else
			{
				System.out.println(" the table is null,please enter the ensure table ");
			}
			return rs;
		}
		
		//This function is added by kanchuanqi 2013.3.18
		public static ResultScanner  getPagePoint(String tableName,int step){
			ResultScanner rs = null;
			if (!tableName.equals(""))
		    {
				try
				{
					HTable table=new HTable(HbaseConf.conf,tableName);  
					Scan scan = new Scan(); 
					PagePoint pp=new PagePoint(step);
					scan.setFilter(pp);
					rs = table.getScanner(scan);
								 
				}catch (IOException e) 
				{ 
					System.out.println(e.getMessage() + "scan failed"); 
				}
		    }//if
			else
			{
				System.out.println(" the table is null,please enter the ensure table ");
			}
			return rs;
		}
		
		public static ArrayList<String> pageFilter(String tableName,int size,String lastRow){
			ArrayList<String> result=new ArrayList<String>();
			final byte[] POSTFIX = new byte[] { 0x00 };  
			ResultScanner scanner = null;
			if (!tableName.equals(""))
		    {
				try
				{
					HTable table=new HTable(HbaseConf.conf,tableName);  
					Scan scan = new Scan(); 
					Filter filter = new PageFilter(size);	
					Filter filter1=new FirstKeyOnlyFilter();
					FilterList filters=new FilterList();
					filters.addFilter(filter);
					filters.addFilter(filter1);
					scan.setFilter(filters);
					if(lastRow != null){  
		                //注意这里添加了POSTFIX操作，不然死循环�? 
		                byte[] startRow = Bytes.add(Bytes.toBytes(lastRow),POSTFIX);  
		                scan.setStartRow(startRow);  
		            }
					scanner = table.getScanner(scan);
					for(Result r:scanner){
						result.add(Bytes.toString(r.getRow()));
					}
				}catch (IOException e) 
				{ 
					System.out.println(e.getMessage() + "scan failed"); 
				}
		    }//if
			else
			{
				System.out.println(" the table is null,please enter the ensure table ");
			}
			
			
			return result;
		}


		public static ArrayList<String>  getQualifierWithEqualFilter(String tableName, String col, String name) throws IOException
		{
			ResultScanner rs = null;
			ArrayList<String> ans = new ArrayList<String>();
			try{
					  HTable table=new HTable(HbaseConf.conf,tableName);  
					  Scan s = new Scan();
					  SingleColumnValueFilter filter = new SingleColumnValueFilter(
							  Bytes.toBytes("xmark"),
							  Bytes.toBytes(col),
							  CompareFilter.CompareOp.EQUAL,
							  new BinaryComparator(Bytes.toBytes(name))
							  );
					  filter.setFilterIfMissing(true);
					  s.setFilter(filter);
	                  rs = table.getScanner(s);
	                  for(Result r:rs)
	                  {
	                	  ans.add(Bytes.toString(r.getValue(Bytes.toBytes("xmark"), Bytes.toBytes(col))));
	                  }//for
					  
					 
				  }//try
				catch (IOException e) 
			  { 
			  // TODO Auto-generated catch block 
			  System.out.println(e.getMessage() + "scan failed"); 
			  }
			return ans;
		}
	     
	     
		
	     
	     
		    /**
			 * Get specified qualifiers of all rows鎻愬彇鎵�湁琛屾墍鏈夌洰鏍囧垪鐨勫�
			 * @param tableName : HBase table name.
			 * @param family : Column Family.
			 * @param qualifiers : A list of qualifiers
			 * @return ResultScanner
			 */
			public static ResultScanner getSpecifiedQualifiers(String tableName, String family, List<String> qualifiers,String startRow,String stopRow){
				if(tableName.isEmpty() || family.isEmpty() || qualifiers.isEmpty())
					return null;
				HTable table = null;
				ResultScanner rs = null;
				try{
					table = new HTable(HbaseConf.conf,tableName);  
					Scan scan = new Scan();	
					if(startRow!=null){
						scan.setStartRow(Bytes.toBytes(startRow));
					}
					if(stopRow!=null){
						scan.setStopRow(Bytes.toBytes(stopRow));
					}
					for(String q : qualifiers){
						scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(q));
					}
					rs = table.getScanner(scan);
					System.out.println("get success!");
				}catch (IOException e) 
				{ 
					System.out.println(e.getMessage() + "scan failed"); 
				}
				return rs;
			}

			/*
			
			public static  String[] IsQualifierValueEqual(String tableName,String family,String qualifer1,String qualifer2) throws IOException
			{
				String rowkey="";
				String value="";
				String [] result={rowkey,value};
				HTable table=new HTable(HbaseConf.conf,tableName);
				 Scan s = new Scan();
				 s.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifer1));
				 s.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifer2));
				 ResultScanner rs = table.getScanner(s);
				   for(Result r:rs)
	                     {
	                       byte[][] test = new byte[r.size()][];   
	                       r.getColumn(arg0, arg1)
					   
	                       }
				 
				return result;
				
			}
	     */
			
			
			
			
			/**
			 * Get specified qualifiers of one row瀹炵幇鎻愬彇鏌愪竴琛屾墍鏈夌洰鏍囧垪鐨勫�?
			 * @param tableName
			 * @param family
			 * @param rowkey
			 * @param xpath
			 * @param p2ctable
			 * @param ans
			 */
		     public static  void getOneSpecifiedQualifiers(String tableName,String family,String rowkey,String xpath, Map<String, List<String>> p2ctable,ArrayList<String> ans)
			    {
			    	if(tableName.isEmpty() || family.isEmpty() )
			    		System.out.println("the tablename or the family is null ,please enter the ensure name!");
			    	HTable table = null;
			    	try
			    	{
			    		List<String> columns = p2ctable.get(xpath);
			    		if(columns!=null)
			    		{
			    		  table = new HTable(HbaseConf.conf,tableName);  
			    		  Get get = new Get(Bytes.toBytes(rowkey)); 	
						  for(String q : columns){  
								get.addColumn(Bytes.toBytes(family), Bytes.toBytes(q));
							}
						  Result rs = table.get(get);
						  //int count=0;
						  StringBuffer temp= new StringBuffer();
						  for(Iterator iter = columns.iterator(); iter.hasNext();)
						  {
							  //++count;
							  String qualifier=iter.next().toString();
							  
	                           byte[] val=rs.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));	
	                           //System.out.println("rowkey="+rowkey+"qualifier="+qualifier+"val"+new String(val));
	                           /*
	                        	  if(Bytes.toString(val)!=null&&(count==columns.size()))
	                        	  {
	                        		 //System.out.println("ans="+Bytes.toString(val));
	                        	   ans.add(Bytes.toString(val));
	                        	  }
	                        	  */
	                           if(Bytes.toString(val)!=null)
	                     	  temp.append(Bytes.toString(val));  
	                     	 
						 }
						  ans.add(temp.toString());
						  
			    		}
			    	}catch (IOException e) 
					{ 
						System.out.println(e.getMessage() + "getOneSpecifiedQualifiers function failed"); 
					}
			    	//return  ans;
			    } 
		     
		     
		     
		     
	     //閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷峰彇鏌愪竴閿熸枻鎷烽敓鍙鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿燂拷
	     
	     public static void getallQualifier(String tableName,String column,String qualifier) throws IOException
	     {
	    	 
	    	 if (!tableName.equals(""))
	         {
	   			  if (!column.equals(""))
	   			  { 
	   				  if(!qualifier.equals(""))
	   				  {
	   					  try
	   					  {
	   						  HTable table = new HTable(HbaseConf.conf, tableName);
	   	                      Scan s = new Scan();
	   	                      s.addColumn(Bytes.toBytes(column), Bytes.toBytes(qualifier));
	   	                      ResultScanner rs = table.getScanner(s);
	   	                    
	   	                      for(Result res:rs)
	   	                     {
	   	                        for(KeyValue kv : res.raw())
	   	                        {
	   	                         System.out.println("rowkey: "+new String(kv.getRow())+"   qualifier="+qualifier+""+ "    value: "+Bytes.toString(kv.getValue()));
	   	                      
	   	                        }//for
	   	                        
	   	                     }//for
	   	                    
	   					  }//try 
	   					catch (IOException e) 
	     				  { 
	     				  // TODO Auto-generated catch block 
	     				  System.out.println(e.getMessage() + "scan failed"); 
	     				  }
	   				 }//if
	   				  else
	   				  {
	   					System.out.println(" the qualifier is null,please enter the ensure qualifier ");
	   				  }
	   			   }//if
	   			  else
	   			  {
					System.out.println(" the family is null,please enter the ensure family");
	   			  }
	   		  	}//if
	    	 else
	    	 {
	    	System.out.println(" the table is null,please enter the ensure table ");
	    	 }
	     }





//閿熸枻鎷烽敓鏂ゆ嫹HbaseInfor閿熸枻鎷烽敓鏂ゆ嫹閿熷彨鍏崇》鎷烽敓鏂ゆ嫹閿熺殕纭锋嫹鍊奸敓鏂ゆ嫹閿熶粙锛涢敓鏂ゆ嫹閿熺煫鐚存嫹閿熸枻鎷峰墠閿熸枻鎷峰閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹閿熼ズ鈭ч獘浼欐嫹閿熸枻鎷锋閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓鏂ゆ嫹

public static void  getallQualifier(HbaseInfor object, ArrayList <String> value) throws IOException
{
	 //ArrayList <String> value = new ArrayList<String>();
	 if (! object.tableName.equals(""))
    {
			  if (! object.tableName.equals(""))
			  { 
				  if(! object.qualifier.equals(""))
				  {
					  try
					  {
						  object.rowKey="";
						  object.value="";
						  HTable table = new HTable(HbaseConf.conf, object.tableName);
	                      Scan s = new Scan();
	                      s.addColumn(Bytes.toBytes( object.column), Bytes.toBytes( object.qualifier));
	                      ResultScanner rs = table.getScanner(s);
	                      for(Result res:rs)
		                     {
		                    	  object.rowKey=new String(res.getRow());
		                    	 // rowKey.add(object.rowKey);
		                    	 //System.out.println("rowkey: "+object.rowKey);
		                        for(KeyValue kv : res.raw())
		                        {
		                        	 object.value=Bytes.toString(kv.getValue());
		                        	 value.add( object.value);
		                        // System.out.println(" rowKey="+ object.rowKey+""+ "    value: "+  object.value);
		                        }//for
		                        
	                     }//for
					  }//try 
					catch (IOException e) 
				  { 
				  // TODO Auto-generated catch block 
				  System.out.println(e.getMessage() + "scan failed"); 
				  }
				 }//if
				  else
				  {
					System.out.println(" the qualifier is null,please enter the ensure qualifier ");
				  }
			   }//if
			  else
			  {
			System.out.println(" the family is null,please enter the ensure family");
			  }
		  	}//if
	 else
	 {
	System.out.println(" the table is null,please enter the ensure table ");
	 }
	 //return value;
  }






public static void  getArrylist(List<String> list)throws IOException
{ 
	Iterator iter=list.iterator();
	for(;iter.hasNext();)
	{
		System.out.println(iter.next()+"");
	}
	
}

public static void getOneSpecifiedQualifiers1(String tableName, String family,
		String rowkey, String xpath, Map<String, List<String>> pctable,
		ArrayList<String> ans) {

	if(tableName.isEmpty() || family.isEmpty() )
		System.out.println("the tablename or the family is null ,please enter the ensure name!");
	HTable table = null;
	try
	{
		List<String> columns = pctable.get(xpath);
		if(columns!=null)
		{
		  table = new HTable(HbaseConf.conf,tableName);  
		  Get get = new Get(Bytes.toBytes(rowkey)); 	
		  for(String q : columns){  
				get.addColumn(Bytes.toBytes(family), Bytes.toBytes(q));
			}
		  Result rs = table.get(get);
		  //int count=0;
		  StringBuffer temp= new StringBuffer();
		  for(Iterator iter = columns.iterator(); iter.hasNext();)
		  {
			  //++count;
			  String qualifier=iter.next().toString();
			  
               byte[] val=rs.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));	
               //System.out.println("rowkey="+rowkey+"qualifier="+qualifier+"val"+new String(val));
               /*
            	  if(Bytes.toString(val)!=null&&(count==columns.size()))
            	  {
            		 //System.out.println("ans="+Bytes.toString(val));
            	   ans.add(Bytes.toString(val));
            	  }
            	  */
               if(Bytes.toString(val)!=null)
         	  temp.append(Bytes.toString(val));  
         	 
		 }
		  ans.add(temp.toString());
		  
		}
	}catch (IOException e) 
	{ 
		System.out.println(e.getMessage() + "getOneSpecifiedQualifiers function failed"); 
	}
	// TODO Auto-generated method stub
	
}

}
	 


