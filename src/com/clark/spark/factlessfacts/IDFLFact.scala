package com.clark.spark.factlessfacts

import java.util.logging.Logger
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.clark.spark.util.CommonUtils._
import com.clark.spark.util.Table
import org.apache.spark.sql.{ SQLContext, SaveMode }

class AggregateIDFLFact extends Table {
  override def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {

    log.info(s"Processing Low Grain ID level for table : " + args(2))

    val adlsPath = getAdlsPath(adlsName)

    ssqc.read.parquet(adlsPath + s"raw/read_json_flat_file").createOrReplaceTempView("read_json_flat_file")  
    val SQL_ = s"select name, customer_id, birthdate from read_json_flat_file"

    ssqc.sql(SQL_BRAND).write.mode(SaveMode.Overwrite).parquet(adlsPath + s"store_management/$client/$brand/lg/brand_dim")
    log.info("Raw to Low Grain processed for table - " + args(2) + s" to " + adlsPath + s"lg/customer_demographic")
  }

}
