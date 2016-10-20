package com.ilimi.employeetracker.main

import java.io.File
import java.io.FileInputStream
import java.util.Properties


object Properties {

	val file = new File("config.properties");
	val fileInput = new FileInputStream(file);
	val properties = new Properties();
	
	   properties.load(fileInput);

	val master=properties.getProperty("master")
	val onlyTimeFormat= properties.getProperty("onlyTimeFormat");
	val onlyDayFormat=properties.getProperty("onlyDayFormat")
	val ipAddress= properties.getProperty("ipAddress");
	val keySpace=properties.getProperty("keyspace")
	val table=properties.getProperty("table")

			fileInput.close();


}