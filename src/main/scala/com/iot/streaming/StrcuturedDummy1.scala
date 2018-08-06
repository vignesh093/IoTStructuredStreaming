package com.iot.streaming


import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

case class Kafkadata(value: String,generated_timestamp: Timestamp)
object StructuredDummy1 {

  val simpleformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

  def convertToTypes(value:Array[String]): (String,Timestamp) ={
    val kafka_value = value(0)
    val generated_timestamp = new Timestamp(simpleformat.parse(value(1)).getTime)
    (kafka_value,generated_timestamp)
  }
  def main(args:Array[String]): Unit ={


    val spark = SparkSession
      .builder
      .appName("kafkalocal1")
        .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sqlContext.setConf("spark.sql.shuffle.partitions","2")
    val lines = spark
      .readStream
      .format("kafka")
      //.option("kafka.bootstrap.servers", "clouderavm01.centralindia.cloudapp.azure.com:9092,clouderavm02.centralindia.cloudapp.azure.com:9092,clouderavm03.centralindia.cloudapp.azure.com:9092")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe", "test2")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
    
     val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()
     
     wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      //.option("checkpointLocation", "checkpointLocation22")
      .start().awaitTermination()
      
     /*wordCounts.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .outputMode("complete")
    .option("checkpointLocation", "new")
    .option("topic", "updates")
    .start().awaitTermination()*/
    //query.awaitTermination()

   /* df.printSchema

    val kafkadf = df.selectExpr("CAST(value as STRING)").as[String].map(value => value.split(",")).map(value => convertToTypes(value))
      .selectExpr("_1 as value","_2 as generated_timestamp").as[Kafkadata]

    val kafkaout = kafkadf.withWatermark("generated_timestamp", "5 minutes")
      .groupBy(
        window($"generated_timestamp", "2 minutes"),
        $"value")
      .count()

    val query = kafkaout.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", false)
      .option("numRows", 10)
     // .option("checkpointLocation", "checkpoint1")
      .start()

    val query = kafkaout.writeStream
      //.outputMode("update")
      .format("csv")
      .option("path", "testfile")
      .option("checkpointLocation", "chk")
      .start()
      

    query.awaitTermination()
*/
  }
}

