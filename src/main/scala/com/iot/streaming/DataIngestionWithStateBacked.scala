/*package com.iot.streaming

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
case class EventDS(connectorid:Integer, deviceid:Integer,eventtime:Timestamp, status:String)
case class DeviceState(var deviceid:Integer, var activeStartTime:Timestamp,var activeEndTime:Timestamp, var deviceactive:String)
case class DeviceInfo(lastactivetime:Timestamp, activetime:Integer)

object DataIngestionWithState {

  val simpleformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

  //{"name": "iottesttimeseries","header": {"timeseries" : 1,"connectorid": 1111,"deviceid": 111,"eventtime": "2018-07-20 13:20:00.000"},"values": [{"variableid": 1,"value": "20"},{"variableid": 2,"value": "30"}]}
   val headerschema = (new StructType)
                 .add("timeseries", IntegerType)
                 .add("connectorid",IntegerType)
                 .add("deviceid",IntegerType)
                 .add("eventtime",TimestampType)
    
    val valueschematimeseries = (new StructType)
                 .add("variableid", IntegerType)
                 .add("value",IntegerType)
            
   val valueschematimeseries =   StructType(Array(
                       StructField("values", ArrayType(StructType(Array(
                       StructField("variableid", IntegerType),
                       StructField("value", IntegerType)))))))
                 
       val schema = StructType(Array(
      StructField("Report", StructType(
        Array(
          StructField("tl", ArrayType(StructType(Array(
            StructField("CID", StringType),
            StructField("TlID", StringType))))))))))
            
   val valueschematimeseries = 
                 
    val timeserieschema = (new StructType)
                 .add("name", StringType)
                 .add("header",headerschema)
                 .add("iotdata",valueschematimeseries)
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
    val headerschema2 = StructType(
        
                          StructField("timeseries", IntegerType) ,
                          StructField("connectorid", IntegerType) ,
                          StructField("deviceid", IntegerType) ,
                          StructField("eventtime", TimestampType) , 
                          Array(StructField("iotdata", iotdatastruct)))

                    val struct =
   StructType(
     StructField("timeseries", IntegerType, true) ::
     StructField("connectorid", IntegerType, false) ::
     StructField("deviceid", IntegerType, false) ::
     StructField("iotdata", iotdatastruct)
     :: Nil)

  val timeserieschema = StructType(
    StructField("name", StringType) ::
      StructField("header", headerschema) ::
      StructField("iotdata", iotdatastruct)
      :: Nil)

  //{"name": "iottestevent","header": {"timeseries" : 0,"connectorid": 1111,"deviceid": 111,"eventtime": "2018-07-20 13:20:00.000"},"values": {"status": "Active"}}

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
  
 def updateUserStateWithEvent(state:DeviceState, input:EventDS): DeviceState={
   
   if(input.status.trim().toUpperCase().equals("ALIVE") && state.deviceactive.equals(""))
     DeviceState(state.deviceid,input.eventtime,state.activeEndTime,"ACTIVE")
     
   else if(input.status.trim().toUpperCase().equals("DEAD") && !state.deviceactive.equals(""))
     DeviceState(state.deviceid,state.activeStartTime,input.eventtime,"DEAD")
   
   else
     state
 }
  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
}
  def updateAcrossEvents( keys:(Integer, Integer), inputs: Iterator[EventDS],oldState: GroupState[DeviceState]):Iterator[DeviceState]={
    //if(!oldState.exists && )
    var vecout = Vector[DeviceState]()
    val inputs_sorted = inputs.toSeq.sortBy { input => input.eventtime }
        var state = if(oldState.exists) oldState.get else DeviceState(keys._2,new Timestamp(System.currentTimeMillis()),new Timestamp(System.currentTimeMillis()),"")
    for(input <- inputs_sorted){  
              println("inputinside ",input)

      if(!(!oldState.exists && input.status.trim().toUpperCase().equals("DEAD"))){

          state = updateUserStateWithEvent(state,input)
          println("Inter state ",state)
          oldState.update(state)
          if(state.deviceactive.equals("DEAD")){
            println("removing state")
            vecout = state +: vecout
            println("outputoutside ",vecout)
            oldState.remove()
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
      .master("local[*]")
      .getOrCreate()
      
    spark.sqlContext.setConf("spark.sql.shuffle.partitions","2")

    import spark.implicits._
    
    //val getmeta = new GetDeviceMetadata
    val devicemeta = GetDeviceMetadata.getmetadata()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "clouderavm01.centralindia.cloudapp.azure.com:9092,clouderavm02.centralindia.cloudapp.azure.com:9092,clouderavm03.centralindia.cloudapp.azure.com:9092")
      //.option("startingOffsets", "latest")
      .option("subscribe", "loadtest")
      .load()

    val devicemetadata = spark.sparkContext.broadcast(devicemeta)

    val KafkaDS = df.selectExpr("CAST(key as STRING)", "CAST(value as STRING)").as[(String, String)]

    val timeseriesDF = KafkaDS.filter(kafkadata => kafkadata._1.split(",")(1).toInt == 1).select(from_json('value, timeserieschema) as 'json_data)
    
    val timeseriesDF_exploded = timeseriesDF.select(timeseriesDF("json_data.name"), timeseriesDF("json_data.header.timeseries"), timeseriesDF("json_data.header.connectorid"),
      timeseriesDF("json_data.header.deviceid"), timeseriesDF("json_data.header.eventtime"), explode(timeseriesDF("json_data.iotdata.values")))

    //timeseriesDF_exploded.printSchema()
    val timeseriesDS = timeseriesDF_exploded.select(timeseriesDF_exploded("name"), timeseriesDF_exploded("timeseries").cast(IntegerType), timeseriesDF_exploded("connectorid"), timeseriesDF_exploded("deviceid"),
      timeseriesDF_exploded("eventtime"), timeseriesDF_exploded("col.variableid"), timeseriesDF_exploded("col.value")).withColumn("result", lit("")).as[TimeseriesDS]
 
    val eventDF = KafkaDS.filter(kafkadata => kafkadata._1.split(",")(1).toInt == 0).select(from_json('value, event_schema) as 'json_data)
    
    val eventDS = eventDF.select(eventDF("json_data.header.connectorid"),eventDF("json_data.header.deviceid"),eventDF("json_data.header.eventtime"),eventDF("json_data.values.status")).as[EventDS]
    
    val event_dataload = eventDS.withWatermark("eventtime", "30 seconds").dropDuplicates("connectorid", "deviceid", "status")
    
    val timeseries_dataload = timeseriesDS.map(data => MetadataValidation(data, devicemetadata)).withWatermark("eventtime", "10 minutes").dropDuplicates("name", "timeseries", "connectorid", "deviceid", "eventtime", "variableid", "value")
    
    //timeseries_dataload.withWatermark("eventtime", "3 minutes").dropDuplicates("name", "timeseries", "connectorid", "deviceid", "eventtime","variableid","value")

    val timeseries_goodrecords = timeseries_dataload.filter(_.result.trim.toLowerCase().equals("success"))

    val timeseries_badrecords = timeseries_dataload.filter(!_.result.trim.toLowerCase().equals("success"))

    val hbasewriter = new ForeachWriter[TimeseriesDS] {
      var putlist_goodrecords = new ListBuffer[Put]
      var putlist_badrecords = new ListBuffer[Put]
      var table: Table = null
      var table1: Table = null
      var connection:Connection = null
      
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

        if(data.result.trim.toLowerCase().equals("success")){
        
        val p = new Put(rowkey)
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("CONNECTORID"), Bytes.toBytes(data.connectorid))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("DEVICEID"), Bytes.toBytes(data.deviceid))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("EVENTTIME"), Bytes.toBytes(eventtime))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VARIABLEID"), Bytes.toBytes(data.variableid))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VALUE"), Bytes.toBytes(data.value))

        putlist_goodrecords += (p)
        
        }
        else{
        
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

    val hbasesink = timeseries_dataload
      .writeStream
      .outputMode("update")
      .foreach(hbasewriter)
      .start()
      
    val kafkadf = df.selectExpr("CAST(value as STRING)").as[String].map(value => value.split(",")).map(value => convertToTypes(value))
      .selectExpr("_1 as value","_2 as generated_timestamp").as[Kafkadata]

    val kafkaout = kafkadf.withWatermark("generated_timestamp", "5 minutes")
      .groupBy(
        window($"generated_timestamp", "2 minutes"),
        $"value")
      .count()

      
      val kafkaout = timeseries_goodrecords.withWatermark("eventtime", "3 minutes")
      .groupBy(
        window($"eventtime", "3 minutes"),
        $"deviceid",
        $"variableid")
      .avg("value")
      
       val kafkaout = timeseries_goodrecords
       
       .withWatermark("eventtime", "3 minutes")
      .groupBy(
        window($"eventtime", "2 minutes"),
        $"deviceid",
        $"variableid")
      .count() 
    
    //val kafkaout12 = timeseries_goodrecords.groupBy("deviceid").count()

    val query = kafkaout
      .map(x => (x(0)+","+x(1)+","+x(2)+","+x(3)))
      .withColumnRenamed("window", "value")
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", "clouderavm01.centralindia.cloudapp.azure.com:9092,clouderavm02.centralindia.cloudapp.azure.com:9092,clouderavm03.centralindia.cloudapp.azure.com:9092")
      .option("topic", "writetopic")
      .option("checkpointLocation", "kafkachk2")
      .start()
      //.agg(max(col("eventtime")).as[Timestamp])
    val maxtimestamp_grouped = eventDS.groupByKey(key => (key.connectorid,key.deviceid))
                                      .flatMapGroupsWithState(
                                                outputMode = OutputMode.Update,
                                                timeoutConf = GroupStateTimeout.NoTimeout)(func = updateAcrossEvents)
                                      //.mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)

    //.withColumn("new", eventDS("eventtime").over(Window.partitionBy("connectorid","deviceid").orderBy("eventtime")))
    maxtimestamp_grouped.printSchema()
    //.withColumn("eventtime", .over(Window.partitionBy("connectorid","deviceid").orderBy("eventtime")))
    //maxtimestamp_grouped.printSchema()
    .groupByKey(key => (key.connectorid,key.deviceid))
    	.mapGroupsWithState[DeviceInfo, DeviceStatus](GroupStateTimeout.NoTimeout()) {
      case((deviceid:Integer ,connectorid:Integer), eventiter:Iterator[eventDS], state: GroupState[DeviceInfo]) =>
        val events = eventiter.toSeq
        val updatedsession = if(state.exists){
          val oldstate = state.get
            for(event <- events){
                if(event.eventtime.after(oldstate.lastactivetime){
                  
                }
            }
        }
    }
      
  val event = query
      .writeStream 
      .format("console")
      .outputMode("update")
      .option("truncate", false)
      .option("numRows", 100)
      .start
      
    val query = kafkaout
      .map(x => (x(0)+","+x(1)+","+x(2)+","+x(3)))
      .withColumnRenamed("window", "value")
      .writeStream  
      .outputMode("update")
      .format("console")
      .option("truncate", false)
      .option("numRows", 100)
      .start
      
    //query.awaitTermination()
    //hbasesink.awaitTermination()
     val query1 = maxtimestamp_grouped
         .writeStream
         .outputMode("update")
         .format("console")
         .option("truncate", false)
         .option("numRows", 100)
         .start()
     query.awaitTermination()
     query1.awaitTermination()
  }
}
*/