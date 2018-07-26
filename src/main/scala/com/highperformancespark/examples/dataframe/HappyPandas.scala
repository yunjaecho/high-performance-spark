package com.highperformancespark.examples.dataframe

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


object HappyPandas {

  case class PandaInfo(
                        place: String,
                        pandaType: String,
                        happyPandas: Integer,
                        totalPandas: Integer)

  /**
    * @param name name of panda
    * @param zip zip code
    * @param pandaSize size of panda in KG
    * @param age age of panda
    */
  case class Pandas(name: String, zip: String, pandaSize: Integer, age: Integer)

  case class Dept(deptNo: Int, dName: String, loc: String)


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

  def sadPandas(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.filter(pandaInfo("happy") !== true)
  }

  def happyFuzzyPandas(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.filter(pandaInfo("happy").and(pandaInfo("attributes")(0) > pandaInfo("attributes")(1)))
  }

  /**
    * Extra the panda info from panda places and compute the squisheness of the panda
    */
  def squishPandaFromPace(pandaPlace: DataFrame): DataFrame = {
    val pandaInfo = pandaPlace.explode(pandaPlace("pandas")){
      case Row(pandas: Seq[Row]) =>
        pandas.map{
          case (Row(
          id: Long,
          zip: String,
          pt: String,
          happy: Boolean,
          attrs: Seq[Double])) =>
            RawPanda(id, zip, pt, happy, attrs.toArray)
        }}
    pandaInfo.select((pandaInfo("attributes")(0) / pandaInfo("attributes")(1)).as("squishyness"))
  }

  /**
    * pandaType을 문자열 대신 정수값으로 변환한다.
    *
    * @param pandaInfo 입련 DataFrame
    * @return 주어진 pandaType에 대한 pandaId와 정수값을 DataFrame 을 변환 한다.
    */
  def econdePandaType(pandaInfo: DataFrame): DataFrame = {
    pandaInfo.select(pandaInfo("id"),
      (when(pandaInfo("pt") === "giant", 0)
      .when(pandaInfo("pt") === "red", 1)
      .otherwise(2).as("encodeType")))
  }

  /**
    * Remove duplicate pandas by id
    * @param pandas
    * @return
    */
  def removeDuplicate(pandas: DataFrame): DataFrame = {
    pandas.dropDuplicates(List("id"))
  }


  def maxPandaSizePerZip(pandas: DataFrame): DataFrame = {
    pandas.groupBy(pandas("zip")).max("pandaSIze")
  }

  def minMeanSizePerZip(pandas: DataFrame): DataFrame = {
    // Compute the min and mean
    pandas.groupBy(pandas("zip")).agg(
      min(pandas("pandaSize")), mean(pandas("pandaSize")))
  }

  def computeRelativePandaSizes(pandas: DataFrame): DataFrame = {
    val windowSpec = Window
      .orderBy(pandas("age"))
      .partitionBy(pandas("zip"))
      .rowsBetween(start = -10, end = 10)

    val pandaRelativeSizCol = pandas("pandaSize") - avg(pandas("pandaSize")).over(windowSpec)

    pandas.select(pandas("name"), pandas("zip"), pandas("pandaSize"), pandas("age"), pandaRelativeSizCol.as("panda_relative_size"))
  }

  def simpleSqlExample(df: DataFrame): Unit = {
    val session = df.sparkSession
    df.registerTempTable("pandas")
    df.write.saveAsTable("perm_pandas")

    session.sql("SELECT * FROM pandas WHERE pandaSize < 12 ").show()
  }


  /**
    * Mysql Table 읽기
    * @param session
    * @return
    */
  def readDatabaseTable(session: SparkSession): DataFrame = {
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "wofl07")
    properties.put("driver", "com.mysql.cj.jdbc.Driver")


    session.read.jdbc("jdbc:mysql://localhost:3306/SCOTT?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "DEPT", properties)
  }

  /**
    * Mysql Table 쓰기
    * @param df
    * @param session
    * @return
    */
  def writeDatabaseTable(df: DataFrame, session: SparkSession): DataFrame = {
    df.write.mode("append")
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/SCOTT?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "wofl07")
      .option("dbtable", "DEPT")
      .save()

    readDatabaseTable(session)
  }


  def writeParquest(df: DataFrame, path: String) = {
    df.write.parquet(path)
  }


}
