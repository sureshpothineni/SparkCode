package com.spark.drw;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class PutExamples {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("drwhd01q:test"));
		
		Put put = new Put(Bytes.toBytes("row1"));
		
		put.addColumn(Bytes.toBytes(""), Bytes.toBytes(""), Bytes.toBytes(""));
		
		table.put(put);
		
		table.close();
		connection.close();
		
	}

}
