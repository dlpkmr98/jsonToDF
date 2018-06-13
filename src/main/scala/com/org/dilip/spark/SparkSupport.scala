package com.org.dilip.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

private [dilip] object SparkSupport {
  
  val conf = new SparkConf().setMaster("local[*]").setAppName("JsonToDF")
  lazy val sc = SparkContext.getOrCreate(conf)
  lazy val sqlCtx = SQLContext.getOrCreate(sc)

}