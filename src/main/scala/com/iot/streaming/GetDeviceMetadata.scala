package com.iot.streaming

import scala.collection.concurrent.TrieMap
import java.io.IOException

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import com.common.utils.SetStreamingProperties

object GetDeviceMetadata {
  
  val metadatamap = new TrieMap[Int,MetadataPOJO]()

  def getmetadata() : TrieMap[Int,MetadataPOJO] =  {
		
		val zkQuorum = SetStreamingProperties.zkQuorum
		val tableName = SetStreamingProperties.metadatatablename
		val conf = HBaseConfiguration.create()
		conf.set("hbase.zookeeper.quorum", zkQuorum)
		conf.set("hbase.zookeeper.property.clientPort", SetStreamingProperties.zkport)
		var connection:Connection = null
		var table:Table = null
		try {
			connection = ConnectionFactory.createConnection(conf)
			table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))
			val scan = new Scan()
			val scanner = table.getScanner(scan)
			val iterator = scanner.iterator()
			while(iterator.hasNext()){
			  val res = iterator.next()
				val metadata = new MetadataPOJO()
				metadata.setDeviceguid(Bytes.toString(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("deviceguid"))))
				metadata.setDeviceid(Bytes.toInt(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("deviceid"))))
				metadata.setConnectorguid(Bytes.toString(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("connectorguid"))))
				metadata.setConnectorid(Bytes.toInt(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("connectorid"))))
				metadata.setVariablename(Bytes.toString(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("variablename"))))
				metadata.setVariableid(Bytes.toInt(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("variableid"))))
				metadata.setVariableDataType(Bytes.toString(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("variableDataType"))))
				metadatamap += (metadata.getDeviceid -> metadata)
			}
		}
		finally {
			if (table != null)
				table.close()
			if (connection != null)
				connection.close()
		}
		metadatamap
		
	}
}