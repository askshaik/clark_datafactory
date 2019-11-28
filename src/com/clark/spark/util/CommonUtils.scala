package com.clark.spark.util

import org.apache.spark.sql.functions.col
import org.apache.hive.common.util.{ Murmur3 => MM3 }
import org.apache.spark.sql.functions._
import scala.util.control._
import scala.util.control.Breaks._
object CommonUtils {

  def flattenDataFrame(spark: SparkSession, nestedDf: DataFrame): DataFrame = {

    var flatCols = Array.empty[String]
    var nestedCols = Array.empty[String]
    var flatDF = spark.emptyDataFrame
    for (w <- nestedDf.dtypes) {
      if (w._2.contains("Struct")) {
        nestedCols = nestedCols.:+(w._1)
      } else {
        flatCols = flatCols.:+(w._1)
      }
    }

    var nestedCol = Array.empty[String]
    for (nc <- nestedCols) {
      for (c <- nestedDf.select(nc + ".*").columns) {
        nestedCol = nestedCol.:+(nc + "." + c)
      }
    }
    val allColumns = flatCols ++ nestedCol
    val colNames = allColumns.map(name => col(name))
    nestedDf.select(colNames: _*)

  }
  def explodeJson(spark: SparkSession, df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df2 = df.withColumn("data", explode($"data"))
      .withColumn("birthdate", $"data"(0))
      .withColumn("customer_id", $"data"(1))
      .withColumn("name", $"data"(2))
      .drop("data")
    df2
  }

  def readJsonFile(spark: SparkSession): DataFrame = {
    spark.read.json("file:\\D:\\Projects\\test-project\\events.json")

  }
  def selectAll(df: DataFrame): Unit = {
    df.select("aggregate_id", "data.*", "id", "timestamp", "type").show()
  }
  def getAdlsPath(adlsName: String, client: String, brand: String): (String, String, String, String) = {

    val adlsRaw = s"adl://$adlsName.azuredatalakestore.net/fishbowl/insights/$client/$brand/raw/"
    val adlsLowGrain = s"adl://$adlsName.azuredatalakestore.net/fishbowl/insights/$client/$brand/lg/"
    val adlsStg = s"adl://$adlsName.azuredatalakestore.net/fishbowl/insights/$client/$brand/stg/"
    val adlsDelta = s"adl://$adlsName.azuredatalakestore.net/fishbowl/insights/$client/$brand/delta_lg/"
    (adlsRaw, adlsLowGrain, adlsStg, adlsDelta)
  }

  def hash64(input: Array[scala.Byte]): Long = {

    MM3.hash64(input)
  }

  def getAdlsPath(adlsName: String): String = {

    //adls Gen 1
    //s"adl://$adlsName.azuredatalakestore.net/fishbowl/"
    //adls Gen 2
    s"abfss://fishbowl@$adlsName.dfs.core.windows.net/"
  }
}
