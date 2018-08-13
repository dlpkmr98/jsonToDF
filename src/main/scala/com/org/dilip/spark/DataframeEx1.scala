package com.org.dilip.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.LongAccumulator

object DataframeEx1 extends App {

  val spark = SparkSupport.spark
  import spark.implicits._

  val accum = spark.sparkContext.longAccumulator("counter")

  /**
   * tupples: Collect as array to small dataframe.
   */
  def tupples = (df1: DataFrame) => df1.map(_.toSeq.toArray.map(_.toString())).collect()
  
  /**
 * @param f:DataFrame => Array[(String, String, String)]
 * @return (DataFrame => A) => (DataFrame => (DataFrame => (LongAccumulator => DataFrame)))
 */
def foldDF[A <: Array[Array[String]]] = (f:DataFrame => A) => 
    (df1: DataFrame) => (df2: DataFrame) => (accum: LongAccumulator) => {
    f(df1).foldLeft(df2)((df, y) => {
      val cnt = accum.value
      val ndf = df.withColumn("status" + y(0) + "_" + cnt,
          when(df(y(0)).geq(y(1).toDouble) and df(y(0)).leq(y(2).toDouble), lit("Y")).otherwise(lit("N")))
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