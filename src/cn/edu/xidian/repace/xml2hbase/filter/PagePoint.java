package cn.edu.xidian.repace.xml2hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;

public class PagePoint extends FilterBase{
	int id;
	int step;
	private boolean foundKV = false;
	//int value;
	public PagePoint(){
		super();
	}
	public PagePoint(int step){
		id=0;
		this.step=step;
		
	}
	
	public boolean filterRowKey(byte[] arg0,int arg1,int arg2){
		id++;
		if(id==step){
			this.id=0;
			return false;
		}
		else{
			return true;
		}
	}
	/*
	public boolean filterRow() {
		//int value=Integer.parseInt(Bytes.toString(this.step));
		id++;
		if(id==step){
			this.id=0;
			return false;
		}
		else{
			return true;
		}
	}
	*/
	public void reset() {
		foundKV = false;
	}
	public ReturnCode filterKeyValue(KeyValue keyValue) {
		if(foundKV) return ReturnCode.NEXT_ROW;
	    foundKV = true;
	    return ReturnCode.INCLUDE;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.step = arg0.readInt();
		this.id=arg0.readInt();
		//this.value=Integer.parseInt(Bytes.toString(this.step));
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		//Bytes.writeByteArray(arg0, this.step);
		arg0.writeInt(step);
		arg0.writeInt(id);
	}

}
