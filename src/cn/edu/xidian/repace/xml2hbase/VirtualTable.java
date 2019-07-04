package cn.edu.xidian.repace.xml2hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *  in VirtualTable,the real path is path ,and the virtual path is *path
 * @author zj
 *
 */
public class VirtualTable {
	private String tableName;
	private Map<String,List<CodeandNumber>> virtualTable = new HashMap<String,List<CodeandNumber>>();
	
	VirtualTable()
	{
		this.tableName = "Undefined";
	}
	
	
	public VirtualTable(String tableName)
	{
		this.tableName = tableName;
	}
	
	String getTableName()
	{
		return this.tableName;
	}
	public boolean hasVirtualPath(String path)
	{
		return virtualTable.containsKey(path);
	}
	

	void insertVirtualTable(String nodePath,CodeandNumber codeandnumber)
	{
		if(!hasVirtualPath(nodePath))
		{
			//private Map<String, List<String>> path2Code;
			List<CodeandNumber> listVirtualCode= new ArrayList<CodeandNumber>(); 
			listVirtualCode.add(codeandnumber);
			virtualTable.put(nodePath,listVirtualCode );
		}
		else
		{
			List<CodeandNumber> list =virtualTable.get(nodePath);
			list.add(codeandnumber);
			virtualTable.put(nodePath, list);
			
		}
	}
	
	public  Map<String,List<CodeandNumber>> getVirtualTable()
	{
		return this.virtualTable;
	}
	
	public List<CodeandNumber> getVirtualPathCode(String path)
	{
		
		return virtualTable.get(path);
	}
	
	public void flushCurrentNumber()
	{// reset all currentNumber 1
		  Set<Entry<String,List<CodeandNumber>>> set = virtualTable.entrySet();   
	      Iterator<Entry<String, List<CodeandNumber>>>   i = set.iterator();   
	      while(i.hasNext()) {   
	             Entry e = i.next();   
	             List<CodeandNumber>valueList = (List<CodeandNumber>) e.getValue(); 
	             for(Iterator<CodeandNumber> itr = valueList.iterator();itr.hasNext();) 
	             {
	            	 CodeandNumber nextObj = itr.next();
	            	 nextObj.changecurrentChildNumber(0);
	            	 
	             }
	      }
		
	}
}
