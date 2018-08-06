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
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.OutputMode

case class TimeseriesDS(name: String, timeseries: Integer, connectorid: Integer, deviceid: Integer, eventtime: Timestamp, variableid: String, value: Double, result: String)
case class EventDS(connectorid: Integer, deviceid: Integer, eventtime: Timestamp, status: String)
case class DeviceState(var deviceid: Integer, var activeStartTime: Timestamp, var activeEndTime: Timestamp, var deviceactive: String) //case class for device state

object DataIngestionWithState {

  val simpleformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS") //dateformat for eventtime

  val headerschema = (new StructType)
    .add("timeseries", IntegerType)
    .add("connectorid", IntegerType)
    .add("deviceid", IntegerType)
    .add("eventtime", TimestampType)

  val iotdatastruct = StructType(
    Array(
      StructField("values", ArrayType(StructType(Array(
        StructField("variableid", StringType),
        StructField("value", DoubleType)))))))

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
    //metadata validtion for every timeseries data
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

    timeseriesdata.copy(result = message) //by using copy we just modify a field result with the message and rest remains same
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

  def updateUserStateWithEvent(state: DeviceState, input: EventDS): DeviceState = {

    //Set the state to ACTIVE only if the first event(identified by "state.deviceactive.equals("")" check) for a device is ALIVE
    if (input.status.trim().toUpperCase().equals("ALIVE") && state.deviceactive.equals(""))
      DeviceState(state.deviceid, input.eventtime, state.activeEndTime, "ACTIVE") //set start time to eventtime and status to ACTIVE

    //Set the state to DEAD only if there is a valid state before for the device(identified by "!state.deviceactive.equals("")" check and if the status is DEAD
    else if (input.status.trim().toUpperCase().equals("DEAD") && !state.deviceactive.equals(""))
      DeviceState(state.deviceid, state.activeStartTime, input.eventtime, "DEAD") //set end time to eventtime and status to ACTIVE

    else //for other cases if any return the state itself
      state
  }

  //ordering for eventtime 
  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  def updateAcrossEvents(keys: (Integer, Integer), inputs: Iterator[EventDS], oldState: GroupState[DeviceState]): Iterator[DeviceState] = {
    //One event for deviceid and connectorid combination
    var vecout = Vector[DeviceState]()
    val inputs_sorted = inputs.toSeq.sortBy { input => input.eventtime } //each deviceid could have multiple events hence sorting by eventtime
    //This ensures that the event messages are in order as emitted by the device
    var state = if (oldState.exists) oldState.get else DeviceState(keys._2, new Timestamp(System.currentTimeMillis()), new Timestamp(System.currentTimeMillis()), "")
    for (input <- inputs_sorted) {
      if (!(!oldState.exists && input.status.trim().toUpperCase().equals("DEAD"))) {
        //if the first event for the deviceid is DEAD we don't process it. A first DEAD event doesn't make sense.
        state = updateUserStateWithEvent(state, input)
        oldState.update(state)
        if (state.deviceactive.equals("DEAD")) {
          //One row is returned only if the event is DEAD for a deviceid and already the deviceid has received ALIVE event(ensured by before if check)
          vecout = state +: vecout
          oldState.remove() //oldState is removed so that next set of ALIVE and DEAD events have start and end time separately based on tonly those events
        }
      }
    }
    vecout.toIterator
  }

  def main(args: Array[String]): Unit = {

    val CF_COLUMN_NAME = Bytes.toBytes("IOT")

    val spark = SparkSession
      .builder
      .appName("DataIngestionWithState")
      //.master("local[*]")
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "2") //neccessary otherwise in local mode job keeps running without output

    import spark.implicits._

    val devicemeta = GetDeviceMetadata.getmetadata()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "clouderavm01.centralindia.cloudapp.azure.com:9092,clouderavm02.centralindia.cloudapp.azure.com:9092,clouderavm03.centralindia.cloudapp.azure.com:9092")
      //.option("startingOffsets", "latest")
      .option("subscribe", "loadtest")
      .load()

    val devicemetadata = spark.sparkContext.broadcast(devicemeta) //device metadata broadcasted

    val KafkaDS = df.selectExpr("CAST(key as STRING)", "CAST(value as STRING)").as[(String, String)]

    //Filter only timeseries data identified by "1"
    val timeseriesDF = KafkaDS.filter(kafkadata => kafkadata._1.split(",")(1).toInt == 1).select(from_json('value, timeserieschema) as 'json_data)

    val timeseriesDF_exploded = timeseriesDF.select(timeseriesDF("json_data.name"), timeseriesDF("json_data.header.timeseries"), timeseriesDF("json_data.header.connectorid"),
      timeseriesDF("json_data.header.deviceid"), timeseriesDF("json_data.header.eventtime"), explode(timeseriesDF("json_data.iotdata.values")))

    val timeseriesDS = timeseriesDF_exploded.select(timeseriesDF_exploded("name"), timeseriesDF_exploded("timeseries").cast(IntegerType), timeseriesDF_exploded("connectorid"), timeseriesDF_exploded("deviceid"),
      timeseriesDF_exploded("eventtime"), timeseriesDF_exploded("col.variableid"), timeseriesDF_exploded("col.value")).withColumn("result", lit("")).as[TimeseriesDS]

    //Filter only event data identified by "0"
    val eventDF = KafkaDS.filter(kafkadata => kafkadata._1.split(",")(1).toInt == 0).select(from_json('value, event_schema) as 'json_data)
    val eventDS = eventDF.select(eventDF("json_data.header.connectorid"), eventDF("json_data.header.deviceid"), eventDF("json_data.header.eventtime"), eventDF("json_data.values.status")).as[EventDS]

    //below line not working as expected
    //val event_dataload = eventDS.withWatermark("eventtime", "30 seconds").dropDuplicates("connectorid", "deviceid", "status")

    //dropping duplicates based on watermark time and keys
    val timeseries_dataload = timeseriesDS.map(data => MetadataValidation(data, devicemetadata)).withWatermark("eventtime", "10 minutes").dropDuplicates("name", "timeseries", "connectorid", "deviceid", "eventtime", "variableid", "value")

    //Filter the records for which the metadata is available(success status) and not-available(other than success)
    val timeseries_goodrecords = timeseries_dataload.filter(_.result.trim.toLowerCase().equals("success"))

    val timeseries_badrecords = timeseries_dataload.filter(!_.result.trim.toLowerCase().equals("success"))

    //hbase sink
    val hbasewriter = new ForeachWriter[TimeseriesDS] {
      var putlist_goodrecords = new ListBuffer[Put]
      var putlist_badrecords = new ListBuffer[Put]
      var table: Table = null
      var table1: Table = null
      var connection: Connection = null

      def open(partitionId: Long, version: Long): Boolean = {
        val zkQuorum = "clouderavm01.centralindia.cloudapp.azure.com,clouderavm02.centralindia.cloudapp.azure.com,clouderavm03.centralindia.cloudapp.azure.com"
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        connection = ConnectionFactory.createConnection(conf)
        table = connection.getTable(TableName.valueOf(Bytes.toBytes("IOTTIMESERIES")))
        table1 = connection.getTable(TableName.valueOf(Bytes.toBytes("IOTTIMESERIES_ERROR")))
        true
      }

      def process(data: TimeseriesDS): Unit = {

        val processed_time = data.eventtime.getTime
        val reverse_timestamp = Long.MaxValue - processed_time

        val rowkey = DataIngestionWithState.PrepareRowKey(data.deviceid, data.connectorid, reverse_timestamp)

        val eventtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(data.eventtime)

        if (data.result.trim.toLowerCase().equals("success")) {

          val p = new Put(rowkey)
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("CONNECTORID"), Bytes.toBytes(data.connectorid))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("DEVICEID"), Bytes.toBytes(data.deviceid))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("EVENTTIME"), Bytes.toBytes(eventtime))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VARIABLEID"), Bytes.toBytes(data.variableid))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VALUE"), Bytes.toBytes(data.value))

          putlist_goodrecords += (p)

        } else {

          val p = new Put(rowkey)
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("CONNECTORID"), Bytes.toBytes(data.connectorid))  
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("DEVICEID"), Bytes.toBytes(data.deviceid))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("EVENTTIME"), Bytes.toBytes(eventtime))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VARIABLEID"), Bytes.toBytes(data.variableid))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VALUE"), Bytes.toBytes(data.value))
          p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("RESULT"), Bytes.toBytes(data.result))

          putlist_badrecords += (p)
        }
      }
      def close(errorOrNull: Throwable): Unit = {
        table.put(putlist_goodrecords.asJava)
        table1.put(putlist_badrecords.asJava)
        table.close
        table1.close
        connection.close
      }
    }

    //Applying a window on deviceid and variableid to calculate the average value for each variable every 3 minutes
    val timeseries_window = timeseries_goodrecords.withWatermark("eventtime", "3 minutes")
      .groupBy(
        window($"eventtime", "3 minutes"),
        $"deviceid",
        $"variableid")
      .avg("value")

    val hbasesink = timeseries_dataload
      .writeStream
      .outputMode("update")
      .foreach(hbasewriter)
      .start()

    //write to kafka sink. For kafka sink "value" field is neccessary hence creating that field by appending other fields with ","
    val windowing = timeseries_window
      .map(x => (x(0) + "," + x(1) + "," + x(2) + "," + x(3)))
      .withColumnRenamed("window", "value")
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", "clouderavm01.centralindia.cloudapp.azure.com:9092,clouderavm02.centralindia.cloudapp.azure.com:9092,clouderavm03.centralindia.cloudapp.azure.com:9092")
      .option("topic", "timeserieswindow")
      .option("checkpointLocation", "timeserieswindowchk")
      .start


    //State processing
    //Event status("ALIVE/DEAD") which is device active status(start-end time) is calculated with the help of state processing.
    //Done for each deviceid and connectorid combination
    //flatMapGroupsWithState is used rather than MapGroupsWithState beacuse we would need an output when we get a "DEAD"(true if the previous event is "ALIVE")
    //whereas MapGroupsWithState will only give a output for each group. So would not work if we get events like ALIVE,DEAD,ALIVE,DEAD in the same group for a deviceid
    val devicestatus = eventDS.groupByKey(key => (key.connectorid, key.deviceid))
      .flatMapGroupsWithState(
        outputMode = OutputMode.Update,
        timeoutConf = GroupStateTimeout.NoTimeout)(func = updateAcrossEvents)

    /*  val event = query
      .writeStream 
      .format("console")
      .outputMode("update")
      .option("truncate", false)
      .option("numRows", 100)
      .start*/

    val deviceevent = devicestatus
      .map(x => (x.deviceid + "," + x.activeStartTime + "," + x.activeEndTime + "," + x.deviceactive))
      .withColumnRenamed("window", "value")
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", "clouderavm01.centralindia.cloudapp.azure.com:9092,clouderavm02.centralindia.cloudapp.azure.com:9092,clouderavm03.centralindia.cloudapp.azure.com:9092")
      .option("topic", "devicestate")
      .option("checkpointLocation", "devicestatechk")
      .start

    /*maxtimestamp_grouped
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", false)
      .option("numRows", 100)
      .start()
*/
  hbasesink.awaitTermination()
  windowing.awaitTermination()
  deviceevent.awaitTermination()
  }
}
