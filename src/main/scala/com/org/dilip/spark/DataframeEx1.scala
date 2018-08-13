package com.org.dilip.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.LongAccumulator

object DataframeEx1 extends App {

  val spark = SparkSupport.spark
  import spark.implicits._

  val accum = spark.sparkContext.longAccumulator("counter")

  def tupples = (df1: DataFrame) => df1.map(x => 
    (x.getString(0), x.getString(1),x.getString(2))).collect()
    
  def foldDF[A <: Array[(String, String, String)]](f:DataFrame => A) = 
    (df1: DataFrame) => (df2: DataFrame) => (accum: LongAccumulator) => {
    f(df1).foldLeft(df2)((df, y) => {
      val cnt = accum.value
      val ndf = df.withColumn("status" + y._1 + "_" + cnt,
          when(df(y._1).geq(y._2.toDouble) and df(y._1).leq(y._3.toDouble), lit("Y")).otherwise(lit("N")))
      accum.add(cnt + 1)
      ndf
    })
  }

  val fDF = Seq(
    ("aclaim", "1", "2.9"),
    ("aclaim", "5", "21"),
    ("bclaim", "23", "30"),
    ("cclaim", "2", "10.1")).toDF("r_element", "min", "max")
    
  val sDF = Seq(
    ("xyz", "1", "0", "0"),
    ("yyz1", "25", "9", "8")).toDF("val1", "aclaim", "bclaim", "cclaim")
 
  val res = foldDF(tupples)(fDF)(sDF)(accum)
  val cols = res.columns.filter(_.contains("status"))
  val fres = res.withColumn("status",
    when(struct(cols.map(col): _*).cast(StringType).contains("Y") === true, "Y").otherwise("N"))
    .drop(cols: _*)

  fres.show(false)

}