package com.qhy.XmarkSearch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import XMarkSearch.XMarkSearch;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseReader;
import cn.edu.xidian.repace.xml2hbase.hbase.HbaseRecreateMappingTable;

public class Query10 {

	private Map<String, List<String>> pctable;
	private String tableName;

	
	public Query10() {
		pctable = null;
		tableName = null;
	}
	
	/**
	 * Constructor with parameters.
	 * @param tableName : the HBase table name .
	 * @param pctable : the path to column mapping table.
	 */
	public Query10(String tableName, Map<String, List<String>> pctable){
		this.pctable = pctable;
		this.tableName = tableName;		
	}
	
	/**
	 * @throws IOException 
	 * @description ��ѯ���
	 */
	public void query() throws IOException
	{
		long timeTestStart=System.currentTimeMillis();
		String personIdPath = "/site/people/person/@id";
		String personCategotyPath = "/site/people/person/profile/interest/@category";
		
		Map<String, List<Personne>> finalResult = new HashMap<String, List<Personne>>();
		Map<String, List<String>> categoryToPersonID = new HashMap<String, List<String>>();
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        List<Filter> filters = new ArrayList<Filter>();
        
        pathColumns.addAll(pctable.get(personIdPath));
        pathColumns.addAll(pctable.get(personCategotyPath));
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL , new BinaryComparator(Bytes.toBytes(column)));
        	filters.add(filter);
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
        ResultScanner rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
        for(Result result : rs)
        {
        	List<KeyValue> keyValues = result.list();
        	Map<String, Integer> categoryMap = new HashMap<String, Integer>();
        	Map<Integer,String>  personMap = new HashMap<Integer, String>();
        	for(int i = 0 ; i < keyValues.size() ; i++)
        	{
        		String[] qualifier = Bytes.toString(keyValues.get(i).getQualifier()).split("[.]");
        		String value = Bytes.toString(keyValues.get(i).getValue());
        		if(qualifier.length == 7)
        		//�����������Ϣ
        		{
        			personMap.put(Integer.parseInt(qualifier[4]), value);
        		}
        		else{
        		//����ǹ�����Ȥ����Ϣ
        			categoryMap.put(value, Integer.parseInt(qualifier[4]));
        		}
        	}
        	for(String category : categoryMap.keySet())
        	{
        		int value = categoryMap.get(category);
        		if(!categoryToPersonID.containsKey(category))
        		{
        			List<String> temp = new ArrayList<String>();
        			categoryToPersonID.put(category, temp);
        		}
        		categoryToPersonID.get(category).add(personMap.get(value));
        	}
        	personMap.clear();
        	categoryMap.clear();
        }
        //���н����
        Map<String, Personne> personsInfo = getPersonInfo();
        WriteRecord.Record("Searhch10-temp", personsInfo.toString());
        for(String category : categoryToPersonID.keySet())
        {
        	for(String person : categoryToPersonID.get(category))
        	{
        		if(!finalResult.containsKey(category))
        		{
        			List<Personne> temp = new ArrayList<Query10.Personne>();
        			temp.add(personsInfo.get(person));
        			finalResult.put(category, temp);
        			temp = null;
        		}else{
        			finalResult.get(category).add(personsInfo.get(person));
        		}
        	}
        }
        personsInfo.clear();
        categoryToPersonID.clear();
        rs.close();
        //��������¼ʱ��
        long timeTestExcute = System.currentTimeMillis();
        WriteRecord.Record("Result10", finalResult.toString());	//������
		try {
			 Date dt=new Date();
		     SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
			WriteRecord.Record("Query10",matter1.format(dt) + "    ��ѯʱ��" + (timeTestExcute - timeTestStart));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 *@time 2016-1-12
	 *@author QiHaiyang
	 *������
	 */
	public static void main(String[] args) {
		String tableName = args[0];
		String P2Ctable = args[1];
		Map<String, List<String>> pctable = null;
		Query10 query = null;
		try {
//			pctable = HbaseRecreateMappingTable.RecP2CMap(P2Ctable, P2Ctable);
			pctable = XMarkSearch.P2C_Map;
			query = new Query10(tableName, pctable);
			for(int i = 0; i < 1; i++){
				query.query();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pctable = null;
	}
	
	/**
	 * @throws IOException 
	 * @description ��ȡ������Ϣ
	 */
	private Map<String, Personne> getPersonInfo() throws IOException
	{
		ResultScanner rs = null;
		
        List<String> pathColumns = new ArrayList<String>();			//����xpath���Ե����ĵ����Ƶı���
        List<Filter> filters = new ArrayList<Filter>();
        Map<String, Personne> personsInfo = new HashMap<String,Personne>();
        
		String path1 = "/site/people/person/@id";				
		List<String> idColumns =  pctable.get(path1);          	pathColumns.addAll(idColumns);
		String path2 = "/site/people/person/profile/gender";		
		List<String> genderColumns =  pctable.get(path2); 		pathColumns.addAll(genderColumns);
		String path3 = "/site/people/person/profile/age";			
		List<String> ageColumns =  pctable.get(path3); 			pathColumns.addAll(ageColumns);
		String path4 = "/site/people/person/profile/education";		
		List<String> educationColumns =  pctable.get(path4);	pathColumns.addAll(educationColumns);
		String path5 = "/site/people/person/profile/@income";		
		List<String> incomeColumns =  pctable.get(path5);		pathColumns.addAll(incomeColumns);
		String path6 = "/site/people/person/name";					
		List<String> nameColumns =  pctable.get(path6);			pathColumns.addAll(nameColumns);
		String path7 = "/site/people/person/address/street";		
		List<String> streetColumns =  pctable.get(path7);		pathColumns.addAll(streetColumns);
		String path8 = "/site/people/person/address/city";			
		List<String> cityColumns =  pctable.get(path8);			pathColumns.addAll(cityColumns);
		String path9 = "/site/people/person/address/country";		
		List<String> countryColumns =  pctable.get(path9);		pathColumns.addAll(countryColumns);
		String path10 = "/site/people/person/emailaddress";			
		List<String> emailaddressColumns =  pctable.get(path10);		pathColumns.addAll(emailaddressColumns);
		String path11 = "/site/people/person/homepage";				
		List<String> homepageColumns =  pctable.get(path11);	pathColumns.addAll(homepageColumns);
		String path12 = "/site/people/person/creditcard";			
		List<String> creditcardColumns =  pctable.get(path12);	pathColumns.addAll(creditcardColumns);
		
        for(String column : pathColumns)
        {
        	Filter filter = new QualifierFilter(CompareOp.EQUAL , new BinaryComparator(Bytes.toBytes(column)));
        	filters.add(filter);
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
        rs = HbaseReader.getRowsWithFilterList(tableName, filterList, null, null);
        
        for(Result result : rs)
        {
        	int position = 1;
        	List<KeyValue> keyValues = result.list();
        	Personne person = new Personne();
        	String id = "";
        	for(KeyValue keyValue : keyValues)
        	{
        		String key = Bytes.toString(keyValue.getQualifier());
        		String[] qualifier = Bytes.toString(keyValue.getQualifier()).split("[.]");
        		String value = Bytes.toString(keyValue.getValue());
        		if(position != Integer.parseInt(qualifier[4]))
        		//���һ���������Ϣ�Ѿ���¼������
        		{
        			personsInfo.put(id, person);	//������Ϣ��Ϣ
        			person = new Personne();		//���½���������Ϣ
        			id = null;
        		}
				if (idColumns.contains(key)) {
					person.setId(value);
					id = value;
				} else if (genderColumns.contains(key)) {
					person.setSexe(value);
				} else if (ageColumns.contains(key)) {
					person.setAge(value);
				} else if (educationColumns.contains(key)) {
					person.setEducation(value);
				} else if (incomeColumns.contains(key)) {
					person.setRevenu(value);
				} else if (nameColumns.contains(key)) {
					person.setNom(value);
				} else if (streetColumns.contains(key)) {
					person.setRue(value);
				} else if (cityColumns.contains(key)) {
					person.setVille(value);
				} else if (countryColumns.contains(key)) {
					person.setPays(value);
				} else if (emailaddressColumns.contains(key)) {
					person.setCourrier(value);
				} else if (homepageColumns.contains(key)) {
					person.setPagePerso(value);
				} else {
					person.setCartePaiement(value);
				}
        		
        		position = Integer.parseInt(qualifier[4]);
        	}
        }
        
		return personsInfo;
	}
	
	/**
	 * @description ��������Ϣ�洢���µ�XML�ļ�֮��
	 * interest �������׼����Ȥ
	 * personne :������Ϣ
	 * file	��Ҫд����ļ���
	 */
	private void ClassToXML(String interest,List<Personne> persons,File file) throws IOException
	{
		File xmlFile = new File("/xmlresult/scaledxml/" + interest + ".xml");
		
		//�����ļ���ڵ�
		Element root = new Element("interest");
		Attribute category = new Attribute("category", interest);
		root.setAttribute(category);
		Document document = new Document(root);	

		for(int i = 0 ; i < persons.size() ; i++)
		{
			Personne personne = persons.get(i);
			
			//��������������Ϣ
			Element personneElement = new Element("personne");
			Element statistiquesElement = new Element("statistiques");
			Element coordonneesElement = new Element("coordonnees");
			Element reseauElement = new Element("reseau");

			
			Attribute idAttribute = new Attribute("id", personne.getId());
			personneElement.setAttribute(idAttribute);			//��������id
			
			if(personne.getSexe() != null)
			{
				Element sexeElement = new Element("sexe");
				sexeElement.setText(personne.getSexe());
				statistiquesElement.setContent(sexeElement);

			}
			if(personne.getAge() != null)
			{
				Element ageElement = new Element("age");
				ageElement.setText(personne.getAge());
				statistiquesElement.setContent(ageElement);

			}
			if(personne.getEducation() != null)
			{
				Element educationElement = new Element("education");
				educationElement.setText(personne.getEducation());
				statistiquesElement.addContent(educationElement);
	
			}
			if(personne.getRevenu() != null)
			{
				Element revenuElement = new Element("revenu");
				revenuElement.setText(personne.getRevenu());
				statistiquesElement.addContent(revenuElement);
			}
			if(personne.getNom() != null)
			{
				Element nomElement = new Element("nom");
				nomElement.setText(personne.getNom());
				coordonneesElement.addContent(nomElement);
			}
			if(personne.getRue() != null)
			{
				Element rueElement = new Element("rue");
				rueElement.setText(personne.getRue());
				coordonneesElement.addContent(rueElement);
			}
			if(personne.getVille() != null)
			{
				Element villeElement = new Element("ville");
				villeElement.setText(personne.getVille());
				coordonneesElement.addContent(villeElement);
			}
			if(personne.getPays() != null)
			{
				Element payseElement = new Element("pays");
				payseElement.setText(personne.getPays());
				reseauElement.addContent(payseElement);
			}
			if(personne.getPagePerso() != null)
			{
				Element pagePersoElement = new Element("pagePerso");
				pagePersoElement.setText(personne.getPagePerso());
				reseauElement.addContent(pagePersoElement);
			}
			if(personne.getCartePaiement() != null)
			{
				Element cartePaiementElement = new Element("cartePaiement");
				cartePaiementElement.setText(personne.getCartePaiement());
				coordonneesElement.addContent(cartePaiementElement);
			}
			
			
			coordonneesElement.addContent(reseauElement);
			personneElement.addContent(statistiquesElement);
			personneElement.addContent(coordonneesElement);
			root.addContent(personneElement);
		}
		
		//�洢���ļ���
		Format format = Format.getPrettyFormat();
		XMLOutputter outputter = new XMLOutputter(format);
		FileOutputStream output = new FileOutputStream(xmlFile);
		outputter.output(document, output);
	}
	
	//���巨��ʽ�ṹ
	class Personne{
		private String id = null;
		private String sexe = null;
		private String age = null;
		private String education = null;
		private String revenu = null;
		private String nom = null;
		private String rue = null;
		private String ville = null;
		private String pays  = null;
		private String courrier = null;
		private String pagePerso = null;
		private String cartePaiement = null;
		
		public void setId(String id)
		{
			this.id = id;
		}
		
		public void setSexe(String sexe)
		{
			this.sexe = sexe;
		}
		
		public void setAge(String age)
		{
			this.age = age;
		}
		
		public void setEducation(String education)
		{
			this.education = education;
		}
		
		public void setRevenu(String revenu)
		{
			this.revenu = revenu;
		}
		
		public void setNom(String nom)
		{
			this.nom = nom;
		}
		
		public void setRue(String rue)
		{
			this.rue = rue;
		}
		
		public void setVille(String ville)
		{
			this.ville = ville;
		}
		
		public void setPays(String pays)
		{
			this.pays = pays;
		}
		
		public void setCourrier(String courrier)
		{
			this.courrier = courrier;
		}
		
		public void setPagePerso(String pagePerso)
		{
			this.pagePerso = pagePerso;
		}
		
		public void setCartePaiement(String cartepaiement)
		{
			this.cartePaiement = cartepaiement;
		}
		
		//get
		public String getId()
		{
			return this.id;
		}
		
		public String getSexe()
		{
			return this.sexe;
		}
		
		public String getAge()
		{
			return this.age;
		}
		
		public String getEducation()
		{
			return this.education;
		}
		
		public String getRevenu()
		{
			return this.revenu;
		}
		
		public String getNom()
		{
			return this.nom;
		}
		
		public String getRue()
		{
			return this.rue;
		}
		
		public String getVille()
		{
			return this.ville;
		}
		
		public String getPays()
		{
			return this.pays;
		}
		
		public String getCourrier()
		{
			return this.courrier;
		}
		
		public String getPagePerso()
		{
			return this.pagePerso;
		}
		
		public String getCartePaiement()
		{
			return this.cartePaiement;
		}
	}
}
