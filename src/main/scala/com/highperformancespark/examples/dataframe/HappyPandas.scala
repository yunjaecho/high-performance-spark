package com.highperformancespark.examples.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


object HappyPandas {
  def sparkSession(): SparkSession = {
    val session = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    // implicits 의 내용을 import 하며 코어 스파크와 달리 context 에 정의 되어 있다.
    import session.implicits._
    session
  }

  def sqlContext(sc: SparkContext): SQLContext = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext
  }

  def hiveContext(sc: SparkContext): HiveContext = {
    val hiveContext = new HiveContext(sc)

    // implicits 의 내용을 import 하며 코어 스파크와 달리 context 에 정의 되어 있다.
    import hiveContext.implicits._
    hiveContext
  }


  def loadDataSimple(sc: SparkContext, session: SparkSession, path: String): DataFrame = {
    val df1 = session.read.json(path)

    val df2 = session.read.format("json")
      .option("samplingRatio", "1.0")
      .load(path)

    val jsonRDD = sc.textFile(path)

    val df3 = session.read.json(jsonRDD)

    df1
  }

  def main(args: Array[String]): Unit = {

  }

}
