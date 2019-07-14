package com.farrellw.helpers

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.joda.time.format._

object helpers {
  val mondayDay: Int = 1
  val thursdayDay: Int = 4
  val minimumInnoHour = 18
  val maximumInnoHour = 21

  def mondayOrThursday(day: Int): Boolean = {
    day == mondayDay || day == thursdayDay
  }

  def withinHourRange(hour: Int): Boolean = {
    hour >= minimumInnoHour && hour <= maximumInnoHour
  }
  def fallsInInnoHours(day: Int, hour: Int): Boolean = {
    mondayOrThursday(day) && withinHourRange(hour)
  }

  def extractTimeUnit(extractionFunction: String => Option[Int]): UserDefinedFunction = {
    udf((dt: String) => {
      dt match {
        case null => None
        case s =>
          extractionFunction(s)
      }
    })
  }

  def extractHour(s: String): Option[Int] = {
    val fmt:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val hardcodedHourOffset: Int = 5
    Some(fmt.parseDateTime(s).getHourOfDay - hardcodedHourOffset)
  }

  def extractDay(s: String): Option[Int] = {
    val fmt:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    Some(fmt.parseDateTime(s).getDayOfWeek)
  }

  def getHour: UserDefinedFunction = extractTimeUnit(extractHour)

  def getDay: UserDefinedFunction = extractTimeUnit(extractDay)
}