package com.hbase.crud;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.*;

public class ValueFilterExamples {
	public static void main(String[] args) throws IOException{
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Table table = connection.getTable(TableName.valueOf("drwhd01q:test"));
		
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("2")));
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		ResultScanner result = table.getScanner(scan);
		System.out.println("Scanning table... ");

		for (Result result2 : result) {
			KeyValue[] keyValue = result2.raw();
			for (KeyValue keyValue2 : keyValue) {
				System.out.println(keyValue2.getFamily());
				System.out.println(keyValue2.getValue());
			}
		}
		
	}
}
