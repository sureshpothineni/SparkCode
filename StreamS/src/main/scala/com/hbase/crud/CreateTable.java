package com.hbase.crud;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

public class CreateTable {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Admin admin = connection.getAdmin();
		//HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("drwhd01q:TestCret"));
		//descriptor.addFamily(new HColumnDescriptor("cf2"));
		
		//admin.createTable(descriptor);
		HColumnDescriptor descriptor = new HColumnDescriptor("cf3");
		admin.addColumn(TableName.valueOf("drwhd01q:TestCret"),  descriptor);
		
		admin.close();
		connection.close();
	}
}
