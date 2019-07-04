package com.qhy.XmarkSearch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import XMarkSearch.XMarkSearch;

public class WriteRecord {
	public static  void Record(String filename,String things) throws IOException
	{
		File file = new File(XMarkSearch.outputPath);
		if(!file.exists())
		{
			file.mkdirs();
		}
		
		File inputFile = new File(XMarkSearch.outputPath + "/" + filename + ".txt");
		FileOutputStream write = new FileOutputStream(inputFile,true);
		write.write(Bytes.toBytes(things));
		write.write(Bytes.toBytes("\r\n"));
		write.close();
	}
}
