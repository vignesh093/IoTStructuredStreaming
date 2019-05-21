package com.iot.streaming

import org.apache.hadoop.hbase.client.Connection
import java.text.SimpleDateFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Table
import scala.collection.JavaConverters._
import com.common.utils.SetStreamingProperties

class HbaseSink(connection: Connection) extends org.apache.spark.sql.ForeachWriter[TimeseriesDS] {
   val CF_COLUMN_NAME = Bytes.toBytes("IOT")
   var putlist = new ListBuffer[Put]
   var table:Table = null

  def open(partitionId: Long, version: Long): Boolean ={
      table = connection.getTable(TableName.valueOf(Bytes.toBytes(SetStreamingProperties.tstablename)))
      true
   }

  def process(data: TimeseriesDS): Unit = {
    
    val processed_time = data.eventtime.getTime
    val reverse_timestamp = Long.MaxValue - processed_time

    val rowkey = DataIngestionWithState.PrepareRowKey(data.deviceid, data.connectorid, reverse_timestamp)

    val eventtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(data.eventtime)
    //name: String, timeseries: Integer, connectorid: Integer, deviceid: Integer, eventtime: Timestamp, variableid: String, value: String, result: String)

    val p = new Put(rowkey)
    p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("CONNECTORID"), Bytes.toBytes(data.connectorid))
    p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("DEVICEID"), Bytes.toBytes(data.deviceid))
    p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("EVENTTIME"), Bytes.toBytes(eventtime))
    p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VARIABLEID"), Bytes.toBytes(data.variableid))
    p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VALUE"), Bytes.toBytes(data.value))

    putlist += (p)
  }

  def close(errorOrNull: Throwable): Unit = {
    table.put(putlist.asJava)
    table.close
  }
}