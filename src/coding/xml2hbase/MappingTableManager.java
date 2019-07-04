package coding.xml2hbase;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element; 
import org.dom4j.io.SAXReader;

public class MappingTableManager {
	

	public static String getColParent(String childCol)
	{
		int offset = childCol.lastIndexOf(".", childCol.length()-1);
		return childCol.substring(0,offset);
	}
	
	private void xmlCode(Element element,String currentRealCode, String currentVirtualCode ,VirtualTable vt, Map<String, List<String>> path2code,Map<String, String> code2Value)
	{
		Element e = null;
		CodeandNumber CandN = new CodeandNumber();
		List<CodeandNumber> CandNList = null;
		int codeListNumber=0;
				
		
		for(Iterator<Element> it = element.elementIterator();it.hasNext();)
		{
			String nextRealCode = "";
			String nextVirtualCode="";
			e = it.next();
			boolean hasPathCode = false;
			CandNList = vt.getVirtualPathCode("*"+e.getPath());
			if(CandNList==null)
			{//virtualTable do not include this path ,need to create a new one 
			
				//get parent infomation
				String parentPath =e.getParent().getPath();
				List<CodeandNumber> parentCandNList = vt.getVirtualPathCode(parentPath);
				if(parentCandNList!=null)
				{//DFS,parent built first ,so parentCandNList must not null
					for(codeListNumber = 0;codeListNumber <parentCandNList.size();codeListNumber++)
					{	
						CodeandNumber parentCandN = parentCandNList.get(codeListNumber);
	                    if(parentCandN.getCode().equals(currentRealCode))
	                    {//注意处理currentRealCode等
	                    	parentCandN.changetotalChildNumber(parentCandN.getTotalChildNumber()+1);
	                    	parentCandN.changecurrentChildNumber(parentCandN.getcurrentChildNumber()+1);	                    	
	                    	//create a new virtualpath
	                    	nextVirtualCode = currentRealCode +"."+Integer.toString(parentCandN.getTotalChildNumber()); 		                
	                    	vt.insertVirtualTable("*"+e.getPath(),new CodeandNumber(nextVirtualCode,1,1,1));
	                    	//create a new realPath
	                    	nextRealCode = nextVirtualCode +".1";	
	                    	vt.insertVirtualTable(e.getPath(),new CodeandNumber(nextRealCode,1,0,0));
	                    	
	                    	List<String> list1 = new ArrayList<String>();
							list1.add(nextRealCode);
							path2code.put(e.getPath(), list1); //insert into p2c
							code2Value.put(nextRealCode, e.getText());//insert into c2v	
							break;
	                    }
					}
				}
				else 
				{
					System.out.println("parent not found");
					System.exit(0);
				}
			}//end e
			
			else if(CandNList!=null)
			{
				for(codeListNumber = 0;codeListNumber < CandNList.size();codeListNumber++)
				{					
					CandN = CandNList.get(codeListNumber);
                    if(getColParent(CandN.getCode()).equals(currentRealCode))
                    {
                    	hasPathCode = true;
                    	break;
                    }
				}
                    
                    if(hasPathCode == true)
					{
                    	//find the same virtual path.judge insert or original
                    	if(CandN.getTotalChildNumber() == CandN.getcurrentChildNumber())
                    	{
                    		//need to insert a new one
                    		//first change the totalchildnumber and the currentchildnumber
                    		CandN.changetotalChildNumber(CandN.getTotalChildNumber()+1);                   		
                    		CandN.changecurrentChildNumber(CandN.getcurrentChildNumber()+1);                 		
                    		nextRealCode = CandN.getCode()+"."+CandN.getcurrentChildNumber();
                    		vt.insertVirtualTable(e.getPath(), new CodeandNumber(nextRealCode,1,0,0));
                    		
                    		nextVirtualCode = CandN.getCode();
                    		List<String>list2 = path2code.get(e.getPath());
    						list2.add(nextRealCode);
    						path2code.put(e.getPath(), list2); //insert into p2c
    						list2 = null;
                    		
                    		code2Value.put(nextRealCode, e.getText());//insert into c2v
                    		
                    	}
                    	else
                    	{
                    		//use the original
                    		//first change the currentchildnumber
                    		String parentPath =e.getParent().getPath();
                    		CandN.changecurrentChildNumber(CandN.getcurrentChildNumber()+1);
                    		if(CandN.getcurrentChildNumber()==1)
                    		{
                    			List<CodeandNumber> parentCandNList = vt.getVirtualPathCode(parentPath);
                    			if(parentCandNList!=null)
                    			{
                    				for(codeListNumber = 0;codeListNumber <parentCandNList.size();codeListNumber++)
                    				{
                    					CodeandNumber parentCandN = parentCandNList.get(codeListNumber);
                    					if(parentCandN.getCode().equals(currentRealCode))
                    					{
                    						parentCandN.changecurrentChildNumber(parentCandN.getcurrentChildNumber()+1);
                    					}
                    				}
                    					
                    			}
                    			
                    		}
                    		nextRealCode = CandN.getCode()+"."+CandN.getcurrentChildNumber(); 
                    		nextVirtualCode = CandN.getCode();
                    		code2Value.put(nextRealCode, e.getText());//insert into c2v
                   	}
					}
					else if(hasPathCode == false)
					{
						//add a new code in  VirtualTable
						
						// create a new code
						String parentPath =e.getParent().getPath();
						List<CodeandNumber> parentCandNList = vt.getVirtualPathCode(parentPath);
						if(parentCandNList!=null)
						{//DFS,parent built first ,so parentCandNList must not null
							for(codeListNumber = 0;codeListNumber <parentCandNList.size();codeListNumber++)
							{				
								CodeandNumber parentCandN = parentCandNList.get(codeListNumber);
			                    if(parentCandN.getCode().equals(currentRealCode))
			                    {//注意处理currentRealCode等
			                    	parentCandN.changetotalChildNumber(parentCandN.getTotalChildNumber()+1);
			                    	parentCandN.changecurrentChildNumber(parentCandN.getcurrentChildNumber()+1);
			                    	//create a new virtualpath
			                    	nextVirtualCode = currentRealCode +"."+Integer.toString(parentCandN.getTotalChildNumber());		                
			                    	vt.insertVirtualTable("*"+e.getPath(),new CodeandNumber(nextVirtualCode ,1,1,1));
			                    	//create a new realPath
			                    	nextRealCode = nextVirtualCode +".1";	
			                    	vt.insertVirtualTable(e.getPath(),new CodeandNumber(nextRealCode,1,0,0));
			                    	List<String> list1 = path2code.get(e.getPath());
									list1.add(nextRealCode);
									path2code.put(e.getPath(), list1); //insert into p2c
									code2Value.put(nextRealCode, e.getText());//insert into c2v	
									break;
			                    }
							}
						}
						else 
						{
							System.out.println("parent not found");
							System.exit(0);
						}	
					
				}
				hasPathCode = false;
				
			}
			xmlCode(e,nextRealCode,nextVirtualCode,vt,path2code,code2Value);	
		}
				
		
		int nAttr = element.attributeCount();
		if(nAttr > 0)
		{
			boolean hasPathCode = false;
			Attribute attr = null;
			List<String> list1 = null;
						
			for(Iterator<?> it = element.attributeIterator();it.hasNext();)
			{
				attr = (Attribute)it.next();
				CandNList = vt.getVirtualPathCode("*"+attr.getPath());
				
				if(CandNList==null)
				{//create a new virtualpath and realpath
					//get parent information
					String parentPath =attr.getParent().getPath();
					List<CodeandNumber> parentCandNList = vt.getVirtualPathCode(parentPath);
					if(parentCandNList!=null)
					{//parent built first ,so parentCandNList must not null
						for(codeListNumber = 0;codeListNumber <parentCandNList.size();codeListNumber++)
						{				
							CodeandNumber parentCandN = parentCandNList.get(codeListNumber);
		                    if(parentCandN.getCode().equals(currentRealCode))
		                    {
		                    	parentCandN.changetotalChildNumber(parentCandN.getTotalChildNumber()+1);
		                    	parentCandN.changecurrentChildNumber(parentCandN.getcurrentChildNumber()+1);
		                    	//create a new virtualpath
		                    	String  nextVirtualCode = currentRealCode +"."+Integer.toString(parentCandN.getTotalChildNumber());		                
		                    	vt.insertVirtualTable("*"+attr.getPath(),new CodeandNumber(nextVirtualCode,1,1,1));
		                    	
		                    	//create a new realPath
		                    	String  nextRealCode = nextVirtualCode +".1";	
		                    	vt.insertVirtualTable(attr.getPath(),new CodeandNumber(nextRealCode,1,0,0));
		                    	
		                    	list1 = new ArrayList<String>();
								list1.add(nextRealCode);
								path2code.put(attr.getPath(), list1); //insert into p2c
								code2Value.put(nextRealCode, attr.getText());//insert into c2v	
								break;
		                    }
						}
					}
					else 
					{
						System.out.println("parent not found");
						System.exit(0);
					}
					
				}//end else if(CandNList==null)
				else if(CandNList!=null)
				{
					//first judge if the same path
					for(codeListNumber = 0;codeListNumber <CandNList.size();codeListNumber++)
					{			
						CandN = CandNList.get(codeListNumber);
	                    if(getColParent(CandN.getCode()).equals(currentRealCode))
	                    {
	                    	hasPathCode = true;
	                    	break;
	                    }
					}
					
					if(hasPathCode == true)
					{//original path exists,because properties is certainly not the same ,so only need to insert into table c2v
						String parentPath =attr.getParent().getPath();
                		CandN.changecurrentChildNumber(CandN.getcurrentChildNumber()+1);
                		if(CandN.getcurrentChildNumber()==1)
                		{
                			List<CodeandNumber> parentCandNList = vt.getVirtualPathCode(parentPath);
                			if(parentCandNList!=null)
                			{
                				for(codeListNumber = 0;codeListNumber <parentCandNList.size();codeListNumber++)
                				{
                					CodeandNumber parentCandN = parentCandNList.get(codeListNumber);
                					if(parentCandN.getCode().equals(currentRealCode))
                					{
                						parentCandN.changecurrentChildNumber(parentCandN.getcurrentChildNumber()+1);
                					}
                				}
                					
                			}
                			
                		}
                		
						String nextRealCode = CandN.getCode()+".1";
						code2Value.put(nextRealCode, attr.getText());//insert into c2v
					}
					
					else if(hasPathCode == false)
					{//not the same parent,so need to create a new one							
						//get parent infomation
						String parentPath =attr.getParent().getPath();
						List<CodeandNumber> parentCandNList = vt.getVirtualPathCode(parentPath);
						if(parentCandNList!=null)
						{//DFS,so parent built first ,so parentCandNList must not null
							for(codeListNumber = 0;codeListNumber <parentCandNList.size();codeListNumber++)
							{		
								
								CodeandNumber parentCandN = parentCandNList.get(codeListNumber);
								
			                    if(parentCandN.getCode().equals(currentRealCode))
			                    {
			                    	parentCandN.changetotalChildNumber(parentCandN.getTotalChildNumber()+1);
			                    	parentCandN.changecurrentChildNumber(parentCandN.getcurrentChildNumber()+1);
			                    	//create a new virtualpath
			                    	String  nextVirtualCode = currentRealCode +"."+Integer.toString(parentCandN.getTotalChildNumber());		                
			                    	vt.insertVirtualTable("*"+attr.getPath(),new CodeandNumber(nextVirtualCode,1,1,1));
			                    	
			                    	//create a new realPath
			                    	String  nextRealCode = nextVirtualCode +".1";	
			                    	vt.insertVirtualTable(attr.getPath(),new CodeandNumber(nextRealCode,1,0,0));
			                    	
			                    	list1 = path2code.get(attr.getPath());
									list1.add(nextRealCode);
									path2code.put(attr.getPath(), list1); //insert into p2c
									code2Value.put(nextRealCode, attr.getText());//insert into c2v	
									break;
			                    }
							}
						}
						else 
						{
							System.out.println("parent not found");
							System.exit(0);
						}													
					}
					hasPathCode = false;
					
				}
		
			}
			
		} //end if(nAttr > 0)		
	}
			
	public void createMappingTable(String xmlPath, String C2ProwName,VirtualTable vt, Map<String, List<String>> path2code ,Map<String, String> code2Value)
	{		
		//Open XML Document
		SAXReader reader = new SAXReader();
		Document doc = null;
		try {
			doc = reader.read(new File(xmlPath));
		} catch (DocumentException e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		if(doc.hasContent())
		{
			Element root = doc.getRootElement();
			
			if(!vt.hasVirtualPath(root.getPath())) //first  article 
				vt.insertVirtualTable(root.getPath(), new CodeandNumber("1",1,0,0));//insert into the VirtualTable

			List<String> rootlist = new ArrayList<String>() ;
			rootlist.add("1");
			path2code.put(root.getPath(),rootlist);//insert into the P2C
			//if(root.getText()!=null)
			code2Value.put("1", root.getText().trim());//insert into the C2V
				
			xmlCode(root,"1","1", vt,path2code,code2Value);
			
		}						
	}	

}
