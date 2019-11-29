package com.clark.spark.dimensions

import java.util.logging.Logger

import com.clark.spark.util.CommonUtils._
import com.clark.spark.util.Table
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.{SQLContext, SaveMode}

class CustomerDemographic extends Table {
  override def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {

    log.info(s"Processing Low Grain to CustomerDemographic for table : " )
    var adlsName = args(3)
    var adlsPath = getAdlsPath(adlsName)
    val customerDemographic = s"select hash64(concat(coalesce(customer_id, 01),coalesce(birthdate,'1900-12-31'), coalesce(name,'')) as customer_hash_key, name, customer_id, birthdate, floor(months_between(current_date,cast(birthdate as date))/12) age_in_year, floor(mod(months_between(current_date,cast(birthdate as date)),12)) age_in_month , floor(DATEDIFF (current_date, add_months(cast(birthdate as date),floor(months_between(current_date,cast(birthdate as date))/12)*12+floor(mod(months_between(current_date,cast(birthdate as date)),12))))) age_in_day ,current_timestamp as etl_created_date, current_timestamp as etl_updated_date, 'clarkadmin' as etl_created_by, 'clarkadmin' as etl_updated_by, 'Nested_Json' as etl_source from read_json_parquet"
    var fileList = 0
    try{
      fileList = dbutils.fs.ls(adlsPath + s"lg/customer_demographic").size
    }
    catch {
      case e: Exception => e.printStackTrace
        log.info("LG is not available")
    }

    if (fileList > 0) {
      ssqc.read.parquet(adlsPath + s"raw/nested_df").createOrReplaceTempView("read_json_parquet")

      ssqc.sql(customerDemographic).write.mode(SaveMode.Append).parquet(adlsPath + s"lg/customer_demographic")
      log.info("Raw to Low Grain processed for table - customer_demographic to " + adlsPath + s"lg/customer_demographic")
    }
    else {
      ssqc.read.parquet(adlsPath + s"raw/nested_df").createOrReplaceTempView("read_json_parquet")

      ssqc.sql(customerDemographic).write.mode(SaveMode.Overwrite).parquet(adlsPath + s"lg/customer_demographic")
      log.info("Raw to Low Grain processed for table - customer_demographic to " + adlsPath + s"lg/customer_demographic")

    }
  }

}
