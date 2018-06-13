package com.org.dilip.spark

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.apache.spark.sql.types._

class JsonToDFTest extends FlatSpec with Matchers {

  lazy val sc = SparkSupport.sc
  lazy val sqlCtx = SparkSupport.sqlCtx

  "This Test" should "return valid dataframe's count" in {
    lazy val jsonPath = "src/test/resources/test.json"
    lazy val dropColumn = Seq("actions", "run")
    val res = JsonToDF().init(JsonToDF.parsedJson, JsonToDF.getWorkFlowDF)(jsonPath)(sc)(sqlCtx)("workflows")(null)(dropColumn)
    res.show(false)
    res.count() shouldBe (2)
  }

  it should "return valid dataframe with given static schema " in {

    lazy val jsonPath = "src/test/resources/test2.json"
    lazy val schema = StructType {
      List(
        StructField("run", IntegerType, true),
        StructField("status", StringType, true))
    }
    val res = JsonToDF().init(JsonToDF.parsedJson, JsonToDF.getWorkFlowDF)(jsonPath)(sc)(sqlCtx)("key")(schema)(null)
    res.show(false)
    res.count() shouldBe (1)
  }
  
}