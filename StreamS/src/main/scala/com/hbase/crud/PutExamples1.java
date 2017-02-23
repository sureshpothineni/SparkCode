package com.hbase.crud;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;


public class PutExamples {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum","lbdp242a.qa.pncint.net,lbdp240a.qa.pncint.net,lbdp241a.qa.pncint.net");
		conf.set("hbase.master","lbdp241a.qa.pncint.net:60000");
		System.out.println("Configuration Done");
		Connection connection = ConnectionFactory.createConnection(conf);
		System.out.println("Connection DONE");
		System.out.println("Configuration :"+connection.getConfiguration());
		//System.out.println("Connection Admin Status:"+connection.getAdmin().getClusterStatus());
		Table table = connection.getTable(TableName.valueOf("drwhd01q:test"));
		System.out.println("Table :"+table.getName());
		System.out.println("Table :"+table.getTableDescriptor());
		System.out.println("Table :"+table.getTableDescriptor().getValue("row1"));
		
		Get get = new Get(Bytes.toBytes("row1"));
		get.addFamily(Bytes.toBytes("cf1"));

		System.out.println("Get a Row row1 :"+get.getId());
		
		Result rs = table.get(get);
		
		System.out.println("Result Set Done");
		System.out.println(" rs values :"+rs.getRow());
		
		//System.out.println(" "+get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
		List<String> values = new ArrayList<String>();
		values.add("Suresh,Babu,Pothineni");
		values.add("S,B,P");
		
		StringBuffer value = new StringBuffer();
		value.append("Suresh,Babu,Pothineni");
		value.append(":");
		value.append("S,B,P");
		Put put = new Put(Bytes.toBytes("row12"));
		System.out.println("Put Command ");
		put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(1));
		System.out.println("Put Command Add Column");
		
		table.put(put);
		System.out.println("table.put done");
		
		table.close();
		connection.close();
		
	}

}
