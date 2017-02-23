package com.hbase.crud;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.conf.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class GetExamples1 {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum","lbdp242a.qa.pncint.net,lbdp240a.qa.pncint.net,lbdp241a.qa.pncint.net");
		conf.set("hbase.master","lbdp241a.qa.pncint.net:60000");
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Table table = connection.getTable(TableName.valueOf("drwhd01q:test"));
		
		table.incrementColumnValue(Bytes.toBytes("row12"), Bytes.toBytes("cf1"), Bytes.toBytes("col9"),1L);

		Get get = new Get(Bytes.toBytes("row11"));
		get.addFamily(Bytes.toBytes("cf1"));
		
		Result r = table.get(get);
		
		/*String col1Value = Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col10")));
		String col2Value = Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col9")));
		
		System.out.println("col1Value :"+col1Value+":col2Value :"+col2Value);
		StringTokenizer tokens = new StringTokenizer(col1Value,":");
		String[] tokenValues = col1Value.split(":");
		for (String string : tokenValues) {
			System.out.println("TokenValue :"+string);
		}
		System.out.println("Cells :"+r.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
		List<KeyValue> keyValues = r.getColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col2"));
		
		for (KeyValue keyValue : keyValues) {
			System.out.println(keyValue.getFamily().toString());
			System.out.println(keyValue.getValue().toString());
		}
		**/
		
		
	}
}
