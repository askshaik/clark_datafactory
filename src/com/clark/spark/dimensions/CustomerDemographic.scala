package com.clark.spark.dimensions

import java.util.logging.Logger

import com.clark.spark.util.CommonUtils._
import com.clark.spark.util.Table
import org.apache.spark.sql.{ SQLContext, SaveMode }

class CustomerDemographic extends Table {
  override def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {

    log.info(s"Processing Low Grain to CustomerDemographic for table : " + args(2))
    val adlsPath = getAdlsPath(adlsName)
    val SQL_BRAND = s"select name, customer_id, birthdate from read_json_flat_file"

    ssqc.read.parquet(adlsPath + s"raw/nested_df").createOrReplaceTempView("read_json_flat_file")   
    ssqc.sql(SQL_BRAND).write.mode(SaveMode.Overwrite).parquet(adlsPath + s"store_management/$client/$brand/lg/brand_dim")
    log.info("Raw to Low Grain processed for table - " + args(2) + s" to " + adlsPath + s"lg/customer_demographic")
  }

}
