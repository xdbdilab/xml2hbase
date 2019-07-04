package coding.xml2hbase;

public class CodeandNumber {
	private String code;
	private int totalNumber;
	private int currentChildNumber;
	private int totalChildNumber;
	
	CodeandNumber()
	{
		this.code = "";
		this.totalNumber  = 0;
		this.currentChildNumber = 0;
		this.totalChildNumber = 0;
	}

	CodeandNumber(String Code ,int totalNumber ,int totalChildNumber,int currentChildNumber)
	{
		this.code = Code;
		this.totalNumber = totalNumber;
		this.currentChildNumber = currentChildNumber;
		this.totalChildNumber = totalChildNumber;
		
	}
	
	String getCode()
	{
		return this.code ;
	}
	
	int getTotalNumber ()
	{
		return this.totalNumber ;
	}
	
	
	int getcurrentChildNumber ()
	{
		return this.currentChildNumber ;
	}
	
	int getTotalChildNumber()
	{
		return this.totalChildNumber;
	}
	
	void changeTotalNumber(int totalNumber)
	{
		this.totalNumber  = totalNumber;
	}
	
	void changecurrentChildNumber(int currentNumber)
	{
		this.currentChildNumber  = currentNumber;
	}
	
	void changetotalChildNumber(int totalChildNumber)
	{
		this.totalChildNumber  = totalChildNumber;
	}

}
