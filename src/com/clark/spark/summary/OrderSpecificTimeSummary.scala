package com.clark.spark.summary
import java.util.logging.Logger

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.clark.spark.util.CommonUtils._
import com.clark.spark.util.Table
import org.apache.spark.sql.{ SQLContext, SaveMode }
class OrderSpecificTimeSummary extends Table {

  override def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {

    log.info(s"Processing Low Grain to OrderSpecificTimeSummary for table : " )
    var adlsName = args(3)
    var adlsPath = getAdlsPath(adlsName)
    val orderSpecificTimeSummary = s"select min(event_timestamp) as  order_accepted_time, max(event_timestamp) as order_fulfilled_time,floor((unix_timestamp(max(event_timestamp)) - unix_timestamp(min(event_timestamp)))/60) as order_completion_in_min,floor((unix_timestamp(max(event_timestamp)) - unix_timestamp(min(event_timestamp)))) as order_completion_in_sec,floor((unix_timestamp(max(event_timestamp)) - unix_timestamp(min(event_timestamp)))/3600) as order_completion_in_hr, aggregate_id from read_json_parquet where type in ('order_accepted','order_fulfilled') group by aggregate_id order by aggregate_id "

    ssqc.read.parquet(adlsPath + s"raw/nested_df").createOrReplaceTempView("read_json_parquet")

      ssqc.sql(orderSpecificTimeSummary).write.mode(SaveMode.Overwrite).parquet(adlsPath + s"lg/order_specific_time_summary")
      log.info("Raw to Low Grain processed for table - customer_demographic to " + adlsPath + s"lg/order_specific_time_summary")

  }
}
