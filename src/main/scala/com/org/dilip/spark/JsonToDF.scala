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
  def init[A, B <: String, C](f: B => A, f1: A => C) = (path: B) => f1(f(path))
}

object JsonToDF {

  def apply() = new JsonToDF

  def parsedJson: String => Option[Map[String, Object]] = p => {
    require(p != null, "path should be present")
    val json = Source.fromFile(p)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    Some(mapper.readValue[Map[String, Object]](json.reader()))
  }

  def getWorkFlowDF = (ob: Option[Map[String, Object]]) => (sc: SparkContext) => (sqlCtx: SQLContext) => (key: String) => (schemas: StructType) => {
    require(key != null, "key should be present")
    val workflows = ob.get.get(key).get.asInstanceOf[List[Map[String, String]]].map(_ - "actions" - "run" - "xyz") //.map(_ filter {case(k,v) => v!=null}) // uncomment if you don't want null as value
    val zipdata = workflows.filter(!_.isEmpty).map(x => x.toList.sortBy(_._1).unzip)
    val defaultSchema = StructType(zipdata.head._1.map(k => StructField(k, StringType, nullable = false))) //only string type is supported. If your want more define your own schema with all Types.
    val schema = Option(schemas).getOrElse(defaultSchema)
    val rows = sc.parallelize(zipdata.map(_._2).map(x => (Row(x: _*))))
    sqlCtx.createDataFrame(rows, schema)
  }

  lazy val jsonPath = "src/main/resources/test.json"
  lazy val sc = SparkSupport.sc
  lazy val sqlCtx = SparkSupport.sqlCtx
  def main(args: Array[String]): Unit = {
    JsonToDF().init(parsedJson, getWorkFlowDF)(jsonPath)(sc)(sqlCtx)("workflows")(null).show(false)
  }

}