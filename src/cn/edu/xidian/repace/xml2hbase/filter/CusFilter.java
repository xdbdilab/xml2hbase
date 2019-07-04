package cn.edu.xidian.repace.xml2hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

public class CusFilter extends FilterBase{
	static final Log LOG = LogFactory.getLog(CusFilter.class);

	protected byte [] columnFamily;
	protected byte [] columnQualifier;
	private CompareOp compareOp;
	private boolean matchedColumn = false;
	//private boolean latestVersionOnly = true;
	protected byte [] value;
	
	public CusFilter(){
		super();
	}
	
	public CusFilter(final byte [] family, final byte [] qualifier,final CompareOp compareOp, final byte [] value){
		this.columnFamily=family;
		this.columnQualifier=qualifier;
		this.compareOp=compareOp;
		this.value=value;
	}
	
	public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
	    Preconditions.checkArgument(filterArguments.size() == 4 || filterArguments.size() == 6,
	                                "Expected 4 but got: %s", filterArguments.size());
	    byte [] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
	    byte [] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
	    CompareOp compareOp = ParseFilter.createCompareOp(filterArguments.get(2));
	    byte [] value1=ParseFilter.removeQuotesFromByteArray(filterArguments.get(3));
	    CusFilter filter = new CusFilter(family, qualifier,compareOp, value1);
	    return filter;
	  }
	
	
	

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.columnFamily = Bytes.readByteArray(arg0);
	    if(this.columnFamily.length == 0) {
	      this.columnFamily = null;
	    }
	    this.columnQualifier = Bytes.readByteArray(arg0);
	    if(this.columnQualifier.length == 0) {
	      this.columnQualifier = null;
	    }
	    this.compareOp = CompareOp.valueOf(arg0.readUTF());
	    this.value=Bytes.readByteArray(arg0);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Bytes.writeByteArray(out, this.columnFamily);
	    Bytes.writeByteArray(out, this.columnQualifier);
	    out.writeUTF(compareOp.name());
	    Bytes.writeByteArray(out, this.value);
	    //Bytes.writeByteArray(out, Bytes.toBytes(this.value+""));
	}

	@Override
	public boolean filterAllRemaining() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ReturnCode filterKeyValue(KeyValue keyValue) {
		// TODO Auto-generated method stub
		//return null;
		/*if (this.matchedColumn) {
		      // We already found and matched the single column, all keys now pass
		      return ReturnCode.INCLUDE;
		    } else if (this.latestVersionOnly && this.foundColumn) {
		      // We found but did not match the single column, skip to next row
		      return ReturnCode.NEXT_ROW;
		    }*/
		    if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
		      return ReturnCode.SKIP;
		    }
		   // foundColumn = true;
		if (filterColumnValue(keyValue.getValue())){
			this.matchedColumn = true;
			 return ReturnCode.INCLUDE;
	    }
	    return ReturnCode.SKIP;
		 
	}
	
	private boolean filterColumnValue( byte [] data){
		int values=Integer.parseInt(Bytes.toString(this.value));
		String reals=Bytes.toString(data);
		int real=Integer.parseInt(reals);
		switch (this.compareOp) {
	    case LESS:
	      return values > real;
	    case LESS_OR_EQUAL:
	      return values >= real;
	    case EQUAL:
	      return values == real;
	    case NOT_EQUAL:
	      return values != real;
	    case GREATER_OR_EQUAL:
	      return values <= real;
	    case GREATER:
	      return values < real;
	    default:
	      throw new RuntimeException("Unknown Compare op " + compareOp.name());
	    }
	}

	@Override
	public boolean filterRow() {
		// TODO Auto-generated method stub
		//return false;
		return  !this.matchedColumn;
	}

	@Override
	public void filterRow(List<KeyValue> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean filterRowKey(byte[] arg0, int arg1, int arg2) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public KeyValue getNextKeyHint(KeyValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasFilterRow() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		//foundColumn = false;
	    matchedColumn = false;
	}

	@Override
	public KeyValue transform(KeyValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}
