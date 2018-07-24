package com.highperformancespark.examples.dataframe

case class RawPanda(id: Long, zip: String, pt: String, happy: Boolean,
                    attributes: Array[Double])

case class PandaPlace(name: String, pandas: Array[RawPanda])
