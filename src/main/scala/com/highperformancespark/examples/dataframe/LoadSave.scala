package com.highperformancespark.examples.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._


case class LoadSave(sc: SparkContext, session: SparkSession) {
  import session.implicits._

  def createFromCaseClassRDD(input: RDD[PandaPlace]) = {
    // Create DataFrame explicitly using session and schema inference
    val df1 = session.createDataFrame(input)

    // create DataFrame using session implicits and schema inference
    val df2 = input.toDF

    val rowRDD = input.map(pm => Row(pm.name,
      pm.pandas.map(pi => Row(pi.id, pi.zip, pi.happy, pi.attributes))))

    val pandasType = ArrayType(StructType(List(
      StructField("id", LongType, true),
      StructField("zip", StringType, true),
      StructField("happy", BooleanType, true),
      StructField("attributes", ArrayType(FloatType), true))))

    // Create DataFrame explicitly with specified schema
    val schema = StructType(List(StructField("name", StringType, true),
      StructField("pandas", pandasType)))

    val df3 = session.createDataFrame(rowRDD, schema)
  }


  def createAndPrintSchema(): DataFrame = {
    val damao = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 1.1))
    val pandaPlace = PandaPlace("toronto", Array(damao))
    val df = session.createDataFrame(Seq(pandaPlace))
    df.printSchema()
    df
  }

  def createRawPandaDataFrame(rawPandas: List[RawPanda]): DataFrame = {
    session.createDataFrame(rawPandas)
  }
}
