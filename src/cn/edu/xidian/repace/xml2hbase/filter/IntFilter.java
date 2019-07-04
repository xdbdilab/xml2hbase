package cn.edu.xidian.repace.xml2hbase.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import com.google.common.base.Preconditions;
public class IntFilter extends FilterBase {
	  static final Log LOG = LogFactory.getLog(IntFilter.class);

	  protected byte [] columnFamily;
	  protected byte [] columnQualifier;
	  private CompareOp compareOp;
	  private WritableByteArrayComparable comparator;
	  private boolean foundColumn = false;
	  private boolean matchedColumn = false;
	  private boolean filterIfMissing = false;
	  private boolean latestVersionOnly = true;
	  //int value;
	  /**
	   * Writable constructor, do not use.
	   */
	  public IntFilter() {
	  }

	  /**
	   
	   * @param family name of column family
	   * @param qualifier name of column qualifier
	   * @param compareOp operator
	   * @param value value to compare column values against
	   */
	  public IntFilter(final byte [] family, final byte [] qualifier,
	      final CompareOp compareOp, final byte[] value) {
	    this(family, qualifier, compareOp, new BinaryComparator(value));
	  }

	  /**
	   
	   * @param family name of column family
	   * @param qualifier name of column qualifier
	   * @param compareOp operator
	   * @param comparator Comparator to use.
	   */
	  public IntFilter(final byte [] family, final byte [] qualifier,
	      final CompareOp compareOp, final WritableByteArrayComparable comparator) {
	    this.columnFamily = family;
	    this.columnQualifier = qualifier;
	    this.compareOp = compareOp;
	    this.comparator = comparator;
	    //value=Integer.parseInt(Bytes.toString(this.comparator.getValue()));
	  }

	  /**
	   * @return operator
	   */
	  public CompareOp getOperator() {
	    return compareOp;
	  }

	  /**
	   * @return the comparator
	   */
	  public WritableByteArrayComparable getComparator() {
	    return comparator;
	  }

	  /**
	   * @return the family
	   */
	  public byte[] getFamily() {
	    return columnFamily;
	  }

	  /**
	   * @return the qualifier
	   */
	  public byte[] getQualifier() {
	    return columnQualifier;
	  }

	  public ReturnCode filterKeyValue(KeyValue keyValue) {
	    
	    if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
	      return ReturnCode.SKIP;
	    }
	    foundColumn = true;
	    if (filterColumnValue(keyValue.getBuffer(),
	        keyValue.getValueOffset(), keyValue.getValueLength())) {
	    	this.matchedColumn = true;
	      return  ReturnCode.INCLUDE;
	    }
	    return ReturnCode.SKIP;
	  }

	  private boolean filterColumnValue(final byte [] data, final int offset,
	      final int length) {
	    //int compareResult = this.comparator.compareTo(data, offset, length);
	    
	    int value=Integer.parseInt(Bytes.toString(this.comparator.getValue()));
	    int real=Integer.parseInt(Bytes.toString(data, offset, length));
	    switch (this.compareOp) {
	    case LESS:
	      return value > real;
	    case LESS_OR_EQUAL:
	      return value >= real;
	    case EQUAL:
	      return value == real;
	    case NOT_EQUAL:
	      return value != real;
	    case GREATER_OR_EQUAL:
	      return value <= real;
	    case GREATER:
	      return value < real;
	    default:
	      throw new RuntimeException("Unknown Compare op " + compareOp.name());
	    }
	  }

	  public boolean filterRow() {
	    // If column was found, return false if it was matched, true if it was not
	    // If column not found, return true if we filter if missing, false if not
	    return  !this.matchedColumn;
	  }

	  public void reset() {
	    foundColumn = false;
	    matchedColumn = false;
	  }

	  /**
	   * Get whether entire row should be filtered if column is not found.
	   * @return true if row should be skipped if column not found, false if row
	   * should be let through anyways
	   */
	  public boolean getFilterIfMissing() {
	    return filterIfMissing;
	  }

	  /**
	   * Set whether entire row should be filtered if column is not found.
	   * <p>
	   * If true, the entire row will be skipped if the column is not found.
	   * <p>
	   * If false, the row will pass if the column is not found.  This is default.
	   * @param filterIfMissing flag
	   */
	  public void setFilterIfMissing(boolean filterIfMissing) {
	    this.filterIfMissing = filterIfMissing;
	  }

	  public boolean getLatestVersionOnly() {
	    return latestVersionOnly;
	  }

	  public void setLatestVersionOnly(boolean latestVersionOnly) {
	    this.latestVersionOnly = latestVersionOnly;
	  }

	  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
	    Preconditions.checkArgument(filterArguments.size() == 4 || filterArguments.size() == 6,
	                                "Expected 4 or 6 but got: %s", filterArguments.size());
	    byte [] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
	    byte [] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
	    CompareOp compareOp = ParseFilter.createCompareOp(filterArguments.get(2));
	    WritableByteArrayComparable comparator = ParseFilter.createComparator(
	      ParseFilter.removeQuotesFromByteArray(filterArguments.get(3)));

	    if (comparator instanceof RegexStringComparator ||
	        comparator instanceof SubstringComparator) {
	      if (compareOp != CompareOp.EQUAL &&
	          compareOp != CompareOp.NOT_EQUAL) {
	        throw new IllegalArgumentException ("A regexstring comparator and substring comparator " +
	                                            "can only be used with EQUAL and NOT_EQUAL");
	      }
	    }

	    IntFilter filter = new IntFilter(family, qualifier,
	                                                                 compareOp, comparator);

	    if (filterArguments.size() == 6) {
	      boolean filterIfMissing = ParseFilter.convertByteArrayToBoolean(filterArguments.get(4));
	      boolean latestVersionOnly = ParseFilter.convertByteArrayToBoolean(filterArguments.get(5));
	      filter.setFilterIfMissing(filterIfMissing);
	      filter.setLatestVersionOnly(latestVersionOnly);
	    }
	    return filter;
	  }

	  public void readFields(final DataInput in) throws IOException {
	    this.columnFamily = Bytes.readByteArray(in);
	    if(this.columnFamily.length == 0) {
	      this.columnFamily = null;
	    }
	    this.columnQualifier = Bytes.readByteArray(in);
	    if(this.columnQualifier.length == 0) {
	      this.columnQualifier = null;
	    }
	    this.compareOp = CompareOp.valueOf(in.readUTF());
	    this.comparator =
	      (WritableByteArrayComparable)HbaseObjectWritable.readObject(in, null);
	    this.foundColumn = in.readBoolean();
	    this.matchedColumn = in.readBoolean();
	    this.filterIfMissing = in.readBoolean();
	    this.latestVersionOnly = in.readBoolean();
	  }

	  public void write(final DataOutput out) throws IOException {
	    Bytes.writeByteArray(out, this.columnFamily);
	    Bytes.writeByteArray(out, this.columnQualifier);
	    out.writeUTF(compareOp.name());
	    HbaseObjectWritable.writeObject(out, comparator,
	        WritableByteArrayComparable.class, null);
	    out.writeBoolean(foundColumn);
	    out.writeBoolean(matchedColumn);
	    out.writeBoolean(filterIfMissing);
	    out.writeBoolean(latestVersionOnly);
	  }

	  @Override
	  public String toString() {
	    return String.format("%s (%s, %s, %s, %s)",
	        this.getClass().getSimpleName(), Bytes.toStringBinary(this.columnFamily),
	        Bytes.toStringBinary(this.columnQualifier), this.compareOp.name(),
	        Bytes.toStringBinary(this.comparator.getValue()));
	  }
	}

