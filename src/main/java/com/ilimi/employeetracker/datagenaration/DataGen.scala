package com.ilimi.employeetracker.datagenaration

import scala.util.Random
import java.io.FileWriter
import org.joda.time.DateTime
import scala.collection.mutable.ArrayBuffer
import org.joda.time.Period
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File

object DataGen {

	var loginTime = ArrayBuffer[String]()
			var logoutTime = ArrayBuffer[String]()
			var empid= ArrayBuffer[String]()
			var EMPID=ArrayBuffer[String]()
			var totallogin = ArrayBuffer[String]()
			var totallogout = ArrayBuffer[String]()
			var LoginTime_emp= ArrayBuffer[String]()
			var LogoutTime_emp= ArrayBuffer[String]()

			def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] ={
		Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
	}

	def  timeConversion(totalSeconds: Int):String= {

		val MINUTES_IN_AN_HOUR = 60;
		val  SECONDS_IN_A_MINUTE = 60;

		val seconds = totalSeconds % SECONDS_IN_A_MINUTE;
		val totalMinutes = totalSeconds / SECONDS_IN_A_MINUTE;
		val minutes = totalMinutes % MINUTES_IN_AN_HOUR;
		val hours = totalMinutes / MINUTES_IN_AN_HOUR;
		return hours + ":" + minutes + ":" + seconds.toString() ;
	}
	val range = dateRange(DateTime.now().minusDays(90),DateTime.now(),Period.days(1)).toArray

			def main(args: Array[String]) {

		for( employee <- 1 to 2){// no of employees

			val listOfWorkingDaysPerEmployee = 45 to 90

					val WorkingDaysPerEmployee = Random.shuffle(listOfWorkingDaysPerEmployee.toList).head //picking no of days present to office each employee

					for(j<- 1 to WorkingDaysPerEmployee){


						val k=1

								val listOfLoginsLogoutsEachDay= 2 to 7   //no of login/logouts each day


								val LoginsLogoutsEachDay = Random.shuffle(listOfLoginsLogoutsEachDay.toList).head

								val timeSequencePerDay= 25200 to 86399
								var timeSlice = Random.shuffle(timeSequencePerDay.toList).take(2*LoginsLogoutsEachDay)
								timeSlice= timeSlice.sorted
								val k2 = (1 to 2*LoginsLogoutsEachDay-1 by 2)

								for( k1 <- k2){

									loginTime+=range(j).toYearMonthDay().toString()+" "+timeConversion(timeSlice(k1-1))

											logoutTime+=range(j).toYearMonthDay().toString()+" "+timeConversion(timeSlice(k1))

											totallogin=loginTime ++  totallogin

											totallogout= logoutTime ++ totallogout

								}

						empid=ArrayBuffer.fill((totallogin.length))(employee.toString())

					}

			EMPID=EMPID ++ empid

					LoginTime_emp=LoginTime_emp++totallogin

					LogoutTime_emp=LogoutTime_emp++totallogout
		}

		val addingColumns= EMPID zip LoginTime_emp zip LogoutTime_emp

				val mapping=addingColumns.map{case (((a,b),c))=>(a,b,c)}

		val content = mapping.toList

				var sb = new StringBuilder();

		for(z <- 1 to content.size - 1 ) {
			sb.append(content(z)._1);
			sb.append(",");
			sb.append(content(z)._2)
			sb.append(",");
			sb.append(content(z)._3)
			sb.append('\n')
		}

		val bwr = new BufferedWriter(new FileWriter(new File("//home//yuva//Desktop//timeinoffice.csv")));

		//write contents of StringBuffer to a file
		bwr.write(sb.toString());

		//flush the stream
		bwr.flush();

		//close the stream
		bwr.close();
	}

}