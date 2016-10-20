package com.ilimi.employeetracker.service

import java.io.IOException

import scala.collection.mutable.Buffer
import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.datastax.spark.connector.toRDDFunctions
import com.ilimi.employeetracker.utils.PropertyReader
import com.ilimi.employeetracker.utils.DateTimeUtils

case class AvgTimeInOffice(empid: String, avgtimeperday: Long, avgtimeperweek: Long, avgtimepermonth: Long, avgtimepertwomonths: Long, avgtimeperthreemonths: Long, avgtimeabsentpermonth: Long, predectedfirstlogin: String)

object EmployeeTrackingService {

  val configuration = new SparkConf(true).set("spark.cassandra.connection.host", PropertyReader.getProperty("ipAddress")).setMaster(PropertyReader.getProperty("master"))
  val sc = new SparkContext("local", "test", configuration)
  val timeInOffice = sc.textFile("timeinoffice2.csv").map(_.split(","))
  val timeInOfficeWithTimediff = timeInOffice.map { x => (x(0), DateTimeUtils.strDateToDay(x(1)), DateTimeUtils.strDateToDay(x(2)), DateTimeUtils.strDateTotimeInSeconds(x(2)) - DateTimeUtils.strDateTotimeInSeconds(x(1)), DateTimeUtils.strToOnlyTimeInSeconds(x(1))) }
  val broadCastListOfDatesInString = sc.broadcast(timeInOffice.map { x => DateTimeUtils.strDateTotimeInSeconds(x(1)) }.collect())
  val broadCastStartDate = sc.broadcast(DateTimeUtils.epocTimeToDate(broadCastListOfDatesInString.value.min))
  val broadCastEndDate = sc.broadcast(DateTimeUtils.epocTimeToDate(broadCastListOfDatesInString.value.max))
  val broadCastNumberOfWeeks = sc.broadcast(DateTimeUtils.noOfWeeksBetweenTwoDates(broadCastStartDate.value, broadCastEndDate.value))
  val broadCastnumberofWeekDays = sc.broadcast(DateTimeUtils.calculateWeekDays(broadCastStartDate.value, broadCastEndDate.value))
  val broadCastnumberOfMonths = sc.broadcast(DateTimeUtils.noOfMonthsBetweenTwoDates(broadCastStartDate.value, broadCastEndDate.value))

  def timeInOfficeBussinessLogic {

    try {

      val totslemployeeTime = timeInOfficeWithTimediff.map { x => (x._1, Buffer(x)) }
      val reduceByKeyTest = totslemployeeTime.reduceByKey((x, y) => (x ++ y)).mapValues { f =>
        val empid = f.map(f => f._1).head

        val empworkingdays = f.map(f => f._2).distinct.size

        val employeeAbsentdys = (broadCastnumberofWeekDays.value - empworkingdays) / broadCastnumberOfMonths.value

        val totaltimeEachEMP = f.map(f => f._4).sum

        val firstTimeLogin = f.groupBy(f => (f._1, f._2)).mapValues(f => f.map(_._5).min).map(f => f._2)
        val totalFirstLoginTimePerEmployee = firstTimeLogin.sum
        val countOfLoginTimeForEachEmployee = firstTimeLogin.count { x => true }
        val mean = totalFirstLoginTimePerEmployee / countOfLoginTimeForEachEmployee
        val devs = firstTimeLogin.map(x => ((x - mean) * (x - mean)))
        val stddev = Math.sqrt(devs.sum / countOfLoginTimeForEachEmployee)
        val expectedTimeRange = ((mean - stddev) + "," + (mean + stddev)).toString()

        AvgTimeInOffice(empid, totaltimeEachEMP / broadCastnumberofWeekDays.value, totaltimeEachEMP / broadCastNumberOfWeeks.value, totaltimeEachEMP / broadCastnumberOfMonths.value, totaltimeEachEMP / 2, totaltimeEachEMP, employeeAbsentdys, expectedTimeRange)

      }
      val avgTimeInOffice = reduceByKeyTest.map(f => f._2)

      avgTimeInOffice.saveToCassandra(PropertyReader.getProperty("keySpace"), PropertyReader.getProperty("table"))

    } catch {
      case e: IllegalArgumentException => println("illegal arg. exception");
      case e: IllegalStateException    => println("illegal state exception");
      case e: IOException              => println("IO exception" + e.printStackTrace());
      case e: Exception                => println("exception caught: " + e);
    } finally {
      println("hi.....finally")
      broadCastListOfDatesInString.destroy()
      broadCastStartDate.destroy()
      broadCastEndDate.destroy()
      broadCastNumberOfWeeks.destroy()
      broadCastnumberofWeekDays.destroy()
      broadCastnumberOfMonths.destroy()
      sc.stop()

    }

  }

}