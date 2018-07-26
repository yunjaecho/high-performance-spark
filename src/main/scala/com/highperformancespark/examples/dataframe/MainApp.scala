package com.highperformancespark.examples.dataframe

import com.highperformancespark.examples.dataframe.HappyPandas.{Dept, PandaInfo, Pandas}
import com.highperformancespark.examples.dataframe.MainApp.sc
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructField
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object MainApp extends App {
  val conf = new SparkConf().setAppName("HappyPandasApp").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val sparkSession = HappyPandas.sparkSession



  val toronto = "toronto"
  val sandiego = "san diego"
  val virginia = "virginia"
  val pandaInfoList = List(
    PandaInfo(toronto, "giant", 1, 2),
    PandaInfo(sandiego, "red", 2, 3),
    PandaInfo(virginia, "black", 1, 10))

  val rawPandaList = List(
    RawPanda(10L, "94110", "giant", true, Array(1.0, 0.9)),
    RawPanda(11L, "94110", "red", true, Array(1.0, 0.9)),
    RawPanda(11L, "94110", "red", true, Array(1.0, 0.9))
  )

  val pandasList = List(Pandas("bata", "10010", 10, 2),
    Pandas("wiza", "10010", 20, 4),
    Pandas("dabdob", "11000", 8, 2),
    Pandas("hanafy", "11000", 15, 7),
    Pandas("hamdi", "11111", 20, 10))


  //testDropDuplicate()
  //testMaxPandaSizePerZip()
  //describe()
  //testMinMeanSizePerZip()
  //testComputeRelativePandaSizes
  //testSortedPanda
  //testSimpleSqlExample
  //testJsonReader
  //testReadDatabaseTable

  //testWriteDatabaseTable

  //testWriteParquet

  //testCreateFromCaseClassRDD

  //testToRDD

  testCollectDF

  /**
    * 중복되는 판다 ID들 삭제
    */
  def testDropDuplicate(): Unit = {
    val pandas = LoadSave(sc, sparkSession).createRawPandaDataFrame(rawPandaList)
    pandas.show()
    // pandas.dropDuplicates(List("id")).show()
    HappyPandas.removeDuplicate(pandas).show()
  }

  def testMaxPandaSizePerZip(): Unit = {
    val pandas = sparkSession.createDataFrame(pandasList.toSeq)
    HappyPandas.maxPandaSizePerZip(pandas).show()
  }

  /**
    * 전체 DataFrame 에 대해 count, mean, sttdev 등의 일반 통계 계산
    */
  def describe(): Unit = {
    val pandas = sparkSession.createDataFrame(pandasList.toSeq)
    pandas.show()
    val df = pandas.describe()
    df.show()
    println(df.collect())
  }

  /**
    * agg API 를 사요한 예제 집계 연산
    */
  def testMinMeanSizePerZip(): Unit = {
    val pandas = sparkSession.createDataFrame(pandasList.toSeq)
    HappyPandas.minMeanSizePerZip(pandas).show()
  }

  /**
    * 동일 우편번호 안에서 +/-10 마리의 판다에 대한(나이순) 윈도를 사용한 평균과의 차이 계산
    */
  def testComputeRelativePandaSizes() = {
    val pandas = sparkSession.createDataFrame(pandasList.toSeq)
    HappyPandas.computeRelativePandaSizes(pandas).show
  }

  def testSortedPanda() = {
    val pandas = sparkSession.createDataFrame(pandasList.toSeq)
    pandas.orderBy(pandas("pandaSize").asc, pandas("age").desc).show()
  }

  /**
    * 테이블에 등록/저장 질의하기
    */
  def testSimpleSqlExample() = {
    val pandas = sparkSession.createDataFrame(pandasList.toSeq)
    HappyPandas.simpleSqlExample(pandas)
  }

  def testJsonReader() = {
    val path = getClass.getResource("/rawpanda.json")
    val df2 = sparkSession.read.format("json")
      .option("samplingRatio", 1.0)
      //.json(path.getPath)
      .load(path.getPath)

    df2.printSchema()
  }

  /**
    * Mysql 테이블 읽기
    */
  def testReadDatabaseTable() = {
    HappyPandas.readDatabaseTable(sparkSession).show()
  }

  /**
    * Mysql 테이블 쓰기
    */
  def testWriteDatabaseTable() = {
    val depts = List(
      Dept(50, "ACCOUNTING", "NEW YORK"),
      Dept(60, "RESEARCH", "DALLAS"),
      Dept(70, "SALES", "CHICAGO"),
      Dept(80, "OPERATIONS", "BOSTON")
    )

    val df = sparkSession.createDataFrame(depts)
    HappyPandas.writeDatabaseTable(df, sparkSession).show()
  }

  def testWriteParquet() = {
    val depts = List(
      Dept(50, "ACCOUNTING", "NEW YORK"),
      Dept(60, "RESEARCH", "DALLAS"),
      Dept(70, "SALES", "CHICAGO"),
      Dept(80, "OPERATIONS", "BOSTON")
    )

    val df = sparkSession.createDataFrame(depts)
    HappyPandas.writeParquest(df, "data.parquet")
  }

  def testCreateFromCaseClassRDD() = {
    val damao = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 1.1))
    val pandaPlace = PandaPlace("toronto", Array(damao))
    val rdd = sc.makeRDD(Seq(pandaPlace))
    LoadSave(sc, sparkSession).createFromCaseClassRDD(rdd)
  }

  def testToRDD() = {
    val pandas = LoadSave(sc, sparkSession).createRawPandaDataFrame(rawPandaList)
    LoadSave(sc, sparkSession).toRDD(pandas)
  }

  def testCollectDF() = {
    val pandas = LoadSave(sc, sparkSession).createRawPandaDataFrame(rawPandaList)
    val collectData = LoadSave(sc, sparkSession).collectDF(pandas)

    for (data <- collectData) {
      println(data)
    }
  }




  def test1() = {
    val path = getClass.getResource("/rawpanda.json")
    val df1 = HappyPandas.loadDataSimple(sc, sparkSession, path.getPath)
    df1.printSchema()

    println("===========================")
    val df2 = LoadSave(session = sparkSession, sc = sc).createAndPrintSchema()

    val df3 = df2.explode(df2("pandas")){
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
    df3.show()

    println("===========================")
    //HappyPandas.sadPandas(df2)
    println("===========================")
    HappyPandas.squishPandaFromPace(df2).show()


    println("===========================")
    println("======  스파크 SQL에서 if - else")
    HappyPandas.econdePandaType(df3).show()


  }


}
