package coding.xml2hbase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HbaseRecreateMappingTable {
	
	
	public static Map<String,Map<String, List<String>>>  RecP2CMapAll(String tableName) throws IOException
	{
		HbaseInfor object=new HbaseInfor();
		List<String> rowkey =new ArrayList();
		object.tableName=tableName;
		Map<String,Map<String, List<String>>> tableMap=new HashMap<String, Map<String, List<String>>>();
		HbaseReader.getAllRecord(object,rowkey);
		Iterator iter1 = rowkey.iterator();
		for(; iter1.hasNext(); )	
		{    
			String a=iter1.next().toString();
			Map<String, List<String>> path2Code = new HashMap<String, List<String>>();
			tableMap.put(a,path2Code);
			object.rowKey=a;
			List<String> qualifier =new ArrayList();
			List<String> value =new ArrayList();
			HbaseReader.getOneRecord(object,qualifier,value);			    
			Iterator iter2 = qualifier.iterator();
			Iterator iter3 =value.iterator();
			
			for(;iter2.hasNext();)
			{
				List<String> code=new ArrayList();
				String temp[]=iter3.next().toString().split("#");
				for(int i=0;i<temp.length;i++)
				{
				 code.add(temp[i]);
				}
				 path2Code.put(iter2.next().toString(),code);
				
			}
		}
		return tableMap;
		
	}
    
    
    public static void getRecP2CMapAll(Map<String,Map<String, List<String>>> tableMap) throws IOException
    {
    	for(Map.Entry<String,Map<String, List<String>>> entry1:tableMap.entrySet())
		{
			   String rowname=entry1.getKey();
			   Map<String, List<String>> p2c=entry1.getValue();
			   //System.out.println("rowname="+rowname);
				for(Map.Entry<String, List<String>> entry2: p2c.entrySet())
				{
					String key=entry2.getKey();
					List<String> v=entry2.getValue();
			
					System.out.println("key="+key+",  value="+v);
				}
		}
		
    }
     
    
    
    
    
    
    public static Map<String,Map<String, String>>  RecC2PMapAll(String tableName) throws IOException
	{			
		HbaseInfor object=new HbaseInfor();
		List<String> rowkey =new ArrayList();
		object.tableName=tableName;
		HbaseReader.getAllRecord(object,rowkey);
     	Map<String,Map<String,String>> tableMap=new HashMap<String, Map<String, String>>();

		Iterator iter1 = rowkey.iterator();
		
		for(; iter1.hasNext(); )	
		{
			String a=iter1.next().toString();
			Map<String, String> code2Path = new HashMap<String, String>();
			tableMap.put(a,code2Path);	
			object.rowKey=a;
			List<String> qualifier =new ArrayList();
			List<String> value =new ArrayList();
			HbaseReader.getOneRecord(object,qualifier,value);	
			Iterator iter2 = value.iterator(); 
			Iterator iter3 = qualifier.iterator(); 
			for(; iter2.hasNext();)
			{
				String code[]=iter2.next().toString().split("#");
				String temp=iter3.next().toString();
				for(int i=0;i<code.length;i++)
				{
					code2Path.put(code[i],temp);						
				}
			}
		}
		return  tableMap;
		
	}
	
    
    public static void getRecC2PMapAll(Map<String,Map<String,String>>  tableMap)
    {
    	for(Map.Entry<String,Map<String, String>> entry1:tableMap.entrySet())
		{
			   String rowname=entry1.getKey();
			   Map<String, String> p2c=entry1.getValue();
			   //System.out.println("rowname="+rowname);
				for(Map.Entry<String, String> entry2: p2c.entrySet())
				{
					String key=entry2.getKey();
				    String v=entry2.getValue();
			
					System.out.println("key="+key+",  value="+v);
				}
		}
    }
    
    
    
    
    
    public static Map<String, List<String>>  RecP2CMap(String tableName,String rowKey ) throws IOException
	{
    	 Map<String, List<String>> path2Code= new HashMap<String, List<String>>();
		try{
			
				HbaseInfor object=new HbaseInfor();
				List<String> qualifier =new ArrayList();
				List<String> value =new ArrayList();
				object.tableName=tableName;
				object.rowKey=rowKey;
				HbaseReader.getOneRecord(object,qualifier,value);			    
				Iterator iter1 = qualifier.iterator();
				Iterator iter2 =value.iterator();
				
				for(;iter1.hasNext();)
				{
					List<String> code=new ArrayList();
					String temp[]=iter2.next().toString().split("#");
					for(int i=0;i<temp.length;i++)
					{
					 code.add(temp[i]);
					}
					 path2Code.put(iter1.next().toString(),code);
					
				}
				/*
				System.out.println("rowname="+object.rowKey);
				for(Map.Entry<String, List<String>> entry:path2Code.entrySet())
				{
					System.out.println("test!");
					String key=entry.getKey();
					List<String> v=entry.getValue();
					System.out.println("key="+key+",  value="+v);
				}
				*/
				
		}//try 
		catch(IOException e)
		{
			 // TODO Auto-generated catch block 
			  System.out.println(e.getMessage() + "Recreating path2code mapping table failed!"); 
		}
		
		return path2Code;
	}
    
    
    public static void  getRecP2CMap(Map<String, List<String>> path2Code) throws IOException
    {
    	for(Map.Entry<String, List<String>> entry:path2Code.entrySet())
		{
			String key=entry.getKey();
			List<String> v=entry.getValue();
			System.out.println("key="+key+",  value="+v);
		}
    }
    
    
    public static Map<String, String> RecC2PMap( String tableName,String rowKey )throws IOException
	{
    	 Map<String,String> code2Path= new HashMap<String, String>();
		try{
			HbaseInfor object=new HbaseInfor();
			List<String> qualifier =new ArrayList();
			List<String> value =new ArrayList();
			object.tableName=tableName;
			object.rowKey=rowKey;
			HbaseReader.getOneRecord(object,qualifier,value);	
			Iterator iter1 = value.iterator(); 
			Iterator iter2 = qualifier.iterator(); 
			for(; iter1.hasNext();)
			{   
				//System.out.println("test RecC2PMap ");
				String code[]=iter1.next().toString().split("#");
				String temp=iter2.next().toString();
				for(int i=0;i<code.length;i++)
				{
					//System.out.println("test RecC2PMap value");
					code2Path.put(code[i],temp);						
				}
			}		
			
		}
		catch(IOException e)
		{
			 // TODO Auto-generated catch block 
			  System.out.println(e.getMessage() + "Recreating code2path mapping table failed!"); 
		}	
		return code2Path;
		
	}
    
  

  
    public static void getRecC2PMap(Map<String, String> code2Value)throws IOException
    {
    	for(Map.Entry<String,String> entry:code2Value.entrySet())
		{
			String key=entry.getKey();
			String v=entry.getValue();
			System.out.println("key="+key+",  value="+v);
		}
    }
    
    
    
    

}
