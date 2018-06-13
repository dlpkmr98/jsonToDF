package com.org.dilip.spark

import scala.io.Source
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

trait JsonToDFService {
  implicit def typec[A](x: A) = x.asInstanceOf[Option[Map[String, Object]]]
  implicit def intToString(x: Int) = x.toString()
}

class JsonToDF extends JsonToDFService {
  /**
 * @param f: B => A
 * @param f1: A => C
 * @return B => C
 */
def init[A, B <: String, C](f: B => A, f1: A => C) = (path: B) => f1(f(path))
}

object JsonToDF {

  def apply() = new JsonToDF

  /**
 * @return String => Option[Map[String, Object]]
 */
def parsedJson: String => Option[Map[String, Object]] = p => {
    require(p != null, "path should be present")
    val json = Source.fromFile(p)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    Some(mapper.readValue[Map[String, Object]](json.reader()))
  }

  /**
 * @return  Option[Map[String, Object]] => (SparkContext => (SQLContext => (String => (StructType => (Seq[String] => DataFrame)))))
 */
def getWorkFlowDF = (ob: Option[Map[String, Object]]) => (sc: SparkContext) => (sqlCtx: SQLContext) => (key: String) => (schemas: StructType) => (dc:Seq[String]) => {
    require(key != null, "key should be present")
    val workflows = ob.get.get(key).get.asInstanceOf[List[Map[String, String]]].map(_ -- Option(dc).getOrElse(Seq("dilip"))) //.map(_ filter {case(k,v) => v!=null}) // uncomment if you don't want null as value
    val zipdata = workflows.filter(!_.isEmpty).map(x => x.toList.sortBy(_._1).unzip)
    val defaultSchema = StructType(zipdata.head._1.map(k => StructField(k, StringType, nullable = true))) //only string type is supported. If your want more.... So define your own schema with all Types.
    val schema = Option(schemas).getOrElse(defaultSchema)
    val rows = sc.parallelize(zipdata.map(_._2).map(x => (Row(x: _*))))
    sqlCtx.createDataFrame(rows, schema)
  }
}
