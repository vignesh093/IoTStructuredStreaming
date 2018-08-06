name := "IoTStructuredStreaming"

version := "0.1"
import sbtassembly.MergeStrategy

scalaVersion := "2.11.10"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-client
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}