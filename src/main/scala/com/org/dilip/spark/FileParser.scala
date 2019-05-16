package com.org.dilip.spark

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount
import org.apache.spark.launcher.SparkLauncher

object FileParser extends App {

  val spark = SparkSupport.spark
  import spark.sqlContext.implicits._

  val schema = new StructType()
    .add("v1", StringType)
    .add("v2", StringType)
    .add("v3", StringType)

  //read the file as text
  val fulldf = spark.read.text("src/test/resources/input")

  val sep = "\\|" + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

  //find invalid df
  val invalidDF = fulldf.where(size(split($"value", sep)) !== schema.length)

  //find valid df
  val validDFTemp = fulldf.except(invalidDF)
  val splitDF = validDFTemp.withColumn("split", split($"value", sep))

  val validDF = schema.fields.zipWithIndex.foldLeft(splitDF)((df, y) => {
    df.withColumn(y._1.name, $"split"(y._2))
  }).drop($"value").drop($"split")

  validDF.show(false)
  validDF.createOrReplaceTempView("valid")

  val res1 = spark.sql(""" select *, case when rec_count = 1 then '' else 'Y' end as duplicate_flag
         from  (select *, count(*) over (partition by v1,v3)as rec_count from valid)  """)

  val res2 = spark.sql(""" select *, case  when columns > 1 then 'True' else 'False' end as duplicate_flag 
         from (SELECT a.* , COUNT(*) OVER (PARTITION BY v1,v3) AS columns FROM valid a)t  """).drop($"columns")
  
    res1.show(false)
    res2.show(false)
    
   spark.streams.awaitAnyTermination()

  //invalidDF.withColumn("reason", lit("invalid record length")).show(false)

}
