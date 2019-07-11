package com.farrellw.spark

import org.apache.spark.sql.SparkSession

object MyApp {
  val applicationName = "SlackAnalysis"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(applicationName).master("local[1]").getOrCreate()
    val sc = spark.sparkContext

    val path = "/Users/will.farrell/dev/1904/spark-streaming/src/main/resources/test_messages.json"
    val multilineOption = true

    val messagesDF = spark.read.option("multiline",multilineOption).json(path)

    messagesDF.printSchema()
  }
}