package com.farrellw.spark

import org.apache.spark.sql.SparkSession
import com.farrellw.models.SlackMessage


object MyApp {
  val applicationName = "SlackAnalysis"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(applicationName).master("local[1]").getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val path = "/Users/will.farrell/dev/1904/spark-streaming/src/main/resources/test_messages.json"
    val multilineOption = true

    val messagesDF = spark.read.option("multiline", multilineOption).json(path)

    print("***********************************")
    val parsedDF = messagesDF.map(row => {
      val text = row.getAs[String]("text")
      SlackMessage(text, None, None)
    })

    parsedDF.show(5)
  }
}