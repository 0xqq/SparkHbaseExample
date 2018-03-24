package com.aramsoft.spark.hbase

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object DataUpload {


  case class InputRec(JobId: String, JobStartDate: String, JobEndDate: String)
  case class OutputRec(JobId: String, JobStartDate: String, JobEndDate: String, JobStatus: String, Duplicate: Boolean, Exception: Boolean)

  def main(args: Array[String]): Unit = {


    import org.apache.spark.sql.catalyst.ScalaReflection
    import org.apache.spark.sql.types.StructType

    val schema = ScalaReflection.schemaFor[InputRec].dataType.asInstanceOf[StructType]
    val conf = new SparkConf().setMaster("local[2]").setAppName("HBase Insert")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .getOrCreate()
    val path = "/Users/aram/Desktop/test.csv"
    println("Input CSV")
    val baseDF = sparkSession.read.schema(schema).option("inferSchema", "true").option("header", "true").csv(path)
    baseDF.show()
    import org.apache.spark.sql.functions._
    val df2 = baseDF.withColumn("rowId",monotonically_increasing_id())
        .withColumn("datedifference", datediff(col("JobStartDate"), col("JobEndDate")))

    val InflatedDF = df2.withColumn("Exception", when(col("datedifference") > 0, "False").otherwise("True"))
      .withColumn("JobStatus", when(col("datedifference") > 0, "Done").otherwise("Inprogress")).drop(col("datedifference"))
    println("Output to HBASE")
    InflatedDF.show()

    val config = HBaseConfiguration.create()
    val tableName = "table1"
    System.setProperty("user.name", "hdfs")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    config.set("hbase.master", "localhost:16010")
    config.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "localhost")
    config.set("zookeeper.znode.parent", "/hbase")
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    config.set("hbase.table.sanity.checks","false")

    val admin = new HBaseAdmin(config)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.setValue("hbase.table.sanity.checks","false")
      admin.createTable(tableDesc)
    }

    import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog

    InflatedDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "2"))
     .format("org.apache.hadoop.hbase.spark")
      .save()


  }

  def catalog = s"""{
                   |"table":{"namespace":"default", "name":"table1"},
                   |"rowkey":"key",
                   |"columns":{
                   |"rowId":{"cf":"rowkey", "col":"rowId", "type":"string"},
                   |"JobId":{"cf":"cf1", "col":"JobId", "type":"string"},
                   |"JobStartDate":{"cf":"cf2", "col":"JobStartDate", "type":"string"},
                   |"JobEndDate":{"cf":"cf3", "col":"JobEndDate", "type":"string"},
                   |"JobStatus":{"cf":"cf4", "col":"JobStatus", "type":"string"},
                   |"Duplicate":{"cf":"cf5", "col":"Duplicate", "type":"boolean"},
                   |"Exception":{"cf":"cf6", "col":"Exception", "type":"boolean"}
                   |}
                   |}""".stripMargin


}
