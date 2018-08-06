package com.iot.streaming

import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Dataset
import org.apache.spark.broadcast.Broadcast
import scala.collection.concurrent.TrieMap
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import scala.collection.mutable.ListBuffer
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.util.Date
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.TableName
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.Table
import org.apache.spark.sql.ForeachWriter

//case class TimeseriesDS(name: String, timeseries: Integer, connectorid: Integer, deviceid: Integer, eventtime: Timestamp, variableid: String, value: String, result: String)

object StructuredDummy {

  val simpleformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

  val headerschema = (new StructType)
    .add("timeseries", IntegerType)
    .add("connectorid", IntegerType)
    .add("deviceid", IntegerType)
    .add("eventtime", TimestampType)
  
  val iotdatastruct = StructType(
    Array(
      StructField("values", ArrayType(StructType(Array(
        StructField("variableid", StringType),
        StructField("value", StringType)))))))
  
  val timeserieschema = StructType(
    StructField("name", StringType) ::
      StructField("header", headerschema) ::
      StructField("iotdata", iotdatastruct)
      :: Nil)

  val valueschemaevent = (new StructType)
    .add("status", StringType)

  val event_schema = (new StructType)
    .add("name", StringType)
    .add("header", headerschema)
    .add("values", valueschemaevent)

  def MetadataValidation(timeseriesdata: TimeseriesDS, devicemetadata: Broadcast[TrieMap[Int, MetadataPOJO]]): TimeseriesDS = {
    val metadatamap = devicemetadata.value
    var message = ""
    if (metadatamap.contains(timeseriesdata.deviceid)) {
      val conn_id = metadatamap.get(timeseriesdata.deviceid).get.getConnectorid
      if (conn_id == timeseriesdata.connectorid)
        message = "Success"
      else
        message = "Connector id mismatch"
    } else
      message = "deviceid not found in metadata"

    timeseriesdata.copy(result = message)
  }

  def PrepareRowKey(devid: Int, connid: Int, RevTime: Long): Array[Byte] = {

    val devid_str = Integer.toString(devid)
    val saltkey = devid_str.substring(devid_str.length() - 1, devid_str.length()).toByte

    val byte1 = new ByteArrayOutputStream();
    val data = new DataOutputStream(byte1);

    data.writeByte(saltkey)
    data.writeInt(devid);
    data.writeInt(connid);
    data.writeLong(RevTime);

    val p = byte1.toByteArray();
    p
  }

  def main(args: Array[String]): Unit = {

    val CF_COLUMN_NAME = Bytes.toBytes("IOT")

    val spark = SparkSession
      .builder
      .appName("DataIngestionWithState")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //val getmeta = new GetDeviceMetadata
    val devicemeta = GetDeviceMetadata.getmetadata()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "clouderavm01.centralindia.cloudapp.azure.com:9092,clouderavm02.centralindia.cloudapp.azure.com:9092,clouderavm03.centralindia.cloudapp.azure.com:9092")
      .option("subscribe", "loadtest")
      .load()

    val devicemetadata = spark.sparkContext.broadcast(devicemeta)

    val KafkaDS = df.selectExpr("CAST(key as STRING)", "CAST(value as STRING)").as[(String, String)]

    val timeseriesDF = KafkaDS.filter(kafkadata => kafkadata._1.split(",")(1).toInt == 1).select(from_json('value, timeserieschema) as 'json_data)

    val timeseriesDF_exploded = timeseriesDF.select(timeseriesDF("json_data.name"), timeseriesDF("json_data.header.timeseries"), timeseriesDF("json_data.header.connectorid"),
      timeseriesDF("json_data.header.deviceid"), timeseriesDF("json_data.header.eventtime"), explode(timeseriesDF("json_data.iotdata.values")))

    val timeseriesDS = timeseriesDF_exploded.select(timeseriesDF_exploded("name"), timeseriesDF_exploded("timeseries").cast(IntegerType), timeseriesDF_exploded("connectorid"), timeseriesDF_exploded("deviceid"),
      timeseriesDF_exploded("eventtime"), timeseriesDF_exploded("col.variableid"), timeseriesDF_exploded("col.value")).withColumn("result", lit("")).as[TimeseriesDS]

    //TimeseriesDS.map(data => data)

    val eventDF = KafkaDS.filter(kafkadata => kafkadata._1.split(",")(1).toInt == 0).select(from_json('value, event_schema) as 'json_data)

    val timeseries_dataload = timeseriesDS.map(data => MetadataValidation(data, devicemetadata))

    val timeseries_goodrecords = timeseries_dataload.filter(_.result.trim.toLowerCase().equals("success"))

    val timeseries_badrecords = timeseries_dataload.filter(!_.result.trim.toLowerCase().equals("success"))
      
     /* val kafkaout = timeseries_goodrecords.withWatermark("eventtime", "5 minutes")
      .groupBy(
        window($"eventtime", "10 minutes"),
        $"deviceid",
        $"variableid")
      .avg()*/
    
     val kafkaout12 = timeseriesDS.withWatermark("eventtime", "5 minutes")
      .groupBy(
        window($"eventtime", "10 minutes"),
        $"deviceid")
      .count()

    val query = kafkaout12.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", false)
      .option("numRows", 10)
      .start()

    query.awaitTermination()


  }
}
