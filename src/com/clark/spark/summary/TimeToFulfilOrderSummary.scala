package com.clark.spark.summary

import java.util.logging.Logger

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.clark.spark.util.CommonUtils._
import com.clark.spark.util.Table
import org.apache.spark.sql.{ SQLContext, SaveMode }

class TimeToFulfilOrderSummary extends Table{


  override def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {

    log.info(s"Processing Low Grain to TimeToFulfilOrderSummary for table : " )
    var adlsName = args(3)
    var adlsPath = getAdlsPath(adlsName)
    val timeToFulfilOrderSummary = s"select hash64(concat(coalesce(customer_id, 01),coalesce(birthdate,'1900-12-31'), coalesce(name,'')) as customer_hash_key, name, customer_id, birthdate, floor(months_between(current_date,cast(birthdate as date))/12) age_in_year, floor(mod(months_between(current_date,cast(birthdate as date)),12)) age_in_month , floor(DATEDIFF (current_date, add_months(cast(birthdate as date),floor(months_between(current_date,cast(birthdate as date))/12)*12+floor(mod(months_between(current_date,cast(birthdate as date)),12))))) age_in_day ,current_timestamp as etl_created_date, current_timestamp as etl_updated_date, 'clarkadmin' as etl_created_by, 'clarkadmin' as etl_updated_by, 'Nested_Json' as etl_source from read_json_parquet"

    ssqc.read.parquet(adlsPath + s"raw/nested_df").createOrReplaceTempView("read_json_parquet")

    ssqc.sql(timeToFulfilOrderSummary).write.mode(SaveMode.Overwrite).parquet(adlsPath + s"lg/time_to_fulfil_order_summary")
    log.info("Raw to Low Grain processed for table - customer_demographic to " + adlsPath + s"lg/time_to_fulfil_order_summary")

  }

}