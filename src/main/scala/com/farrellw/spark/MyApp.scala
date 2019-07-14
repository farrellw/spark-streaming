package com.farrellw.spark

import com.farrellw.helpers.helpers
import org.apache.spark.sql.SparkSession
import com.farrellw.models.SlackMessage
import org.apache.spark.sql.functions._

object MyApp {
  val applicationName = "SlackAnalysis"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(applicationName).master("local[1]").getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val messagesFilePath = "/Users/will.farrell/dev/1904/spark-streaming/src/main/resources/test_messages.json"
    val multilineOption = true

    //Read a JSON message of 100 slack messagse into spark
    val messagesDF = spark.read.option("multiline", multilineOption).json(messagesFilePath)


    val dateRepresentation = from_unixtime($"ts")

    //Add three columns around dates/time with UserDefinedFunctions and spark.sql function
    val dfWithTimestamp = messagesDF.withColumn("timestamp", dateRepresentation)
    val dfWithHours = dfWithTimestamp.withColumn("hour", helpers.getHour($"timestamp"))
    val dfWithDay = dfWithHours.withColumn("day", helpers.getDay($"timestamp"))

    //Map over data structure to preserve only what needed
    val mappedDF = dfWithDay.map(row => {
      val text = row.getAs[String]("text")

      val date = row.getAs[String]("timestamp")
      val day = row.getAs[Int]("day")
      val hour = row.getAs[Int]("hour")
      SlackMessage(text, None, date, helpers.fallsInInnoHours(day, hour))
    })

    //Filter messages that occurred during innovation hours
    val filteredDF = mappedDF.filter(_.innovationHours)
    filteredDF.show(5)
  }
}