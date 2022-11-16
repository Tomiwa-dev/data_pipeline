package org.example.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.rogach.scallop.ScallopConf

import java.time.format.DateTimeFormatter

object mongoToBigquery {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def createSparkSession(): SparkSession = {
    val appName = "mongoToBigQuery" + DateTimeFormatter.ofPattern("ddMMYYYY_HHmmss").format(java.time.LocalDateTime.now)

    val SparkConf = new SparkConf()
      .setAppName(appName)
//      .set("tempGCSbucket", tempGCSbucket)

    SparkSession.builder().config(SparkConf)
      .getOrCreate()

  }

  def readFromMongo(spark: SparkSession, uri: String, database: String, collection: String): DataFrame = {

    spark.read.format("mongodb")
      .option("connection.uri", uri)
      .option("database", database)
      .option("collection", collection)
      .option("readPreference.name", "secondaryPreferred")
      .option("inferSchema", "true")
      .load()
  }
  class cliParser(args: Seq[String]) extends ScallopConf(args){


    val mongoUri = opt[String](name = "mongoUri", short = 'm', required = true, argName = "mongoUri",descr = "passes in MongoDb uri")
    val database = opt[String](name = "database", short = 'd', required = true, argName = "database",descr = "passes in mongoDB database name")
    val collection = opt[String](name = "collection", short = 'c', required = true, argName = "collection", descr = " passes in mongoDB collection name")
    val tempGCSbucket = opt[String](name = "tempGCSbucket", short = 't', required = true, argName = "temporaryGCSBucket", descr = "passes in a temporary gcs bucket")
    val bigQueryDestination = opt[String](name = "bigQueryDestination", short = 'b', required = true, argName = "bigQueryDestination", descr = "passes in a bigQueryDestination")
    val schema = opt[String](name = "customSchema", short = 's', required = false, argName = "customSchema", descr = "passes in a customSchema")

    verify()
  }

  def main(args: Array[String]): Unit = {

    val cliParser = new cliParser(args)
    val uri = cliParser.mongoUri.getOrElse("")
    val database = cliParser.database.getOrElse("")
    val collection = cliParser.collection.getOrElse("")
    val tempGCSbucket = cliParser.tempGCSbucket.getOrElse("")
    val bigQueryDestination = cliParser.bigQueryDestination.getOrElse(default = "")
    //    val schema = cliParser.schema.getOrElse("")

    //val bucket = "temp_big_query_spark"


    val spark = createSparkSession()
    spark.conf.set("temporaryGcsBucket", tempGCSbucket)
    spark.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.gs.implicit.dir.repair.enable", "false")
    spark.sparkContext.setLogLevel("WARN")

    val df = readFromMongo(spark, uri, database, collection)

    df.write.format("bigquery").mode(SaveMode.Overwrite).option("table", bigQueryDestination).save()


    println("Insert Completed.")

    spark.stop()


  }
}
