package com.ilimi.employeetracker.test

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.scalatest.FlatSpec
import com.datastax.spark.connector._
import com.ilimi.employeetracker.service.EmployeeTrackingService
import com.ilimi.employeetracker.service.EmployeeTrackingService

class TestEmployeeTrackingService extends FlatSpec {

  val test = EmployeeTrackingService.broadCastStartDate

  val sc = EmployeeTrackingService.sc
  val rdd = sc.cassandraTable("excelsior", "word")
  val cassandratableName = rdd.tableName
  val firstEmployeeId = rdd.first().get[String]("empid")
  val predectedfirstlogin = rdd.first().get[String]("predectedfirstlogin")

  val totalRowCount = rdd.count()

  "TimeInOffice" should "pop values" in {

    assert(firstEmployeeId == "7")

  }

  it should "pop row count" in {
    assert(totalRowCount == 23)

  }

  it should "pop predectedfirstlogin  " in {
    assert(predectedfirstlogin == "22420.21319051704,29943.78680948296")

  }
  it should "give cassandra Table name" in {

    assert(cassandratableName == "word")
  }
}