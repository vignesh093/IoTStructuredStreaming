package com.iot.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType

object SparkTest {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("DataIngestionWithState")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

/*    val schema = StructType(Array(
      StructField("Report", StructType(
        Array(
          StructField("tl", ArrayType(StructType(Array(
            StructField("CID", StringType),
            StructField("TlID", StringType))))))))))*/
    
  /*      val schema = StructType(Array(
      StructField("Report", StructType(
        Array(
          StructField("tl", ArrayType(StructType(Array(
            StructField("CID", StringType),
            StructField("TlID", StringType))))))))))*/

    
    val headerschema = (new StructType)
                 .add("timeseries", IntegerType)
                 .add("connectorid",IntegerType)
                 .add("deviceid",IntegerType)
                 .add("eventtime",TimestampType)
    val iotdatastruct =  StructType(
                              Array(
                               StructField("values",ArrayType(StructType(Array(
                                   StructField("variableid", StringType),
                                    StructField("value", StringType)))))))
                                    
    val schema = StructType(
                           StructField("name", StringType) ::
                           StructField("header", headerschema) ::
                           StructField("iotdata", iotdatastruct) 
                           :: Nil)
    
    //val df = Seq("""{"ErrorMessage": null,"IsError": false,"Report":{  "tl":[  {  "TlID":"F6","CID":"mo"},{  "TlID":"Fk","CID":"mo"}]}}""").toDS

    /*  val df = Seq("""{  
  "ErrorMessage": null,
  "IsError": false,
   "Report":{  
      "tl":[  
         {  
            "TlID":"F6",
            "CID":"mo"
         },
         {  
            "TlID":"Fk",
            "CID":"mo"
         }
      ]
   }
}""").toDS*/

    val df = Seq("""{  
  "ErrorMessage": null,
  "IsError": false,
  "Report" : {
      "tl":[  
         {  
            "TlID":"F6",
            "CID":"mo"
         },
         {  
            "TlID":"Fk",
            "CID":"mo"
         }
      ]
}
   
}""").toDS

val df1 =  Seq("""{
	"name": "iottesttimeseries",
	"header": {
		"timeseries" : 1,
		"connectorid": 1111,
		"deviceid": 111,
		"eventtime": "2018-07-20 13:20:00.000"
	},
	"iotdata":{  
	"values": [{
			"variableid": 1,
			"value": "20"
		}
	]
	}
}""").toDS



    spark.read.schema(schema).json(df1).show(false)
  }
}