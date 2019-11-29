package com.clark.spark.summary
import java.util.logging.Logger

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.clark.spark.util.CommonUtils._
import com.clark.spark.util.Table
import org.apache.spark.sql.{ SQLContext, SaveMode }
class ProbabilitySummary extends Table {


  override def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {

    log.info(s"Processing Low Grain to TimeToFulfilOrderSummary for table : " )
    var adlsName = args(3)
    var adlsPath = getAdlsPath(adlsName)
    val probabilitySummary = s"select floor(months_between(current_date,cast(birthdate as date))/12) age_in_year, count(type) from read_json_parquet where type='order_cancelled' group by floor(months_between(current_date,cast(birthdate as date))/12)"

    ssqc.read.parquet(adlsPath + s"raw/nested_df").createOrReplaceTempView("read_json_parquet")

    ssqc.sql(probabilitySummary).write.mode(SaveMode.Overwrite).parquet(adlsPath + s"lg/probability_summary")
    log.info("Raw to Low Grain processed for table - customer_demographic to " + adlsPath + s"lg/probability_summary")

  }
}