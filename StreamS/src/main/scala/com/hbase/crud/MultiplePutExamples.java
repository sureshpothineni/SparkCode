package com.hbase.crud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class MultiplePutExamples {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum","lbdp242a.qa.pncint.net,lbdp240a.qa.pncint.net,lbdp241a.qa.pncint.net");
		conf.set("hbase.master","lbdp241a.qa.pncint.net:60000");
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("drwhd01q:test"));

		List<Put> puts = new ArrayList<Put>();

		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("put_row1"));
		puts.add(put);
		
		Put put1 = new Put(Bytes.toBytes("row3"));
		put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col3"), Bytes.toBytes("put_row3"));
		puts.add(put1);
		
		Put put2 = new Put(Bytes.toBytes("row5"));
		put2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col5"), Bytes.toBytes("put_row5"));
		puts.add(put2);

		Put put3 = new Put(Bytes.toBytes("row6"));
		put3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col6"), Bytes.toBytes("put_row6"));
		puts.add(put3);

		Put put4 = new Put(Bytes.toBytes("row7"));
		put4.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col7"), Bytes.toBytes("put_row7"));
		puts.add(put4);

		table.put(puts);
		table.close();
		connection.close();
		
	}

}
