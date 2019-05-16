package com.org.dilip.spark

import org.apache.spark.sql.functions._
import java.util.Calendar
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.File
import org.apache.spark.sql.catalyst.expressions.Ascending

/**
 * Read top two files from any path
 */
object FileReader extends App {

  val spark = SparkSupport.spark
  //val spark = SparkSupport.

  // val files = spark.sparkContext.wholeTextFiles("C:\\Users\\560414\\Desktop\\data\\abc*")

  // files.map(x => x._1).collect().foreach(println)

  val data = spark
    .read
    .text("C:\\Users\\560414\\Desktop\\data\\abc*")
    .select(input_file_name.as("c1"))//.sort(desc("c1"))
    .rdd
    .map(_.getString(0))
    .take(2)
    
    //data.foreach(println)

  data
    .foreach(x => spark.read.text(x)
      .write
      .text("target/"+new File(x).getName))

}
