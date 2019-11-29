package com.clark.spark.factlessfacts

import java.util.logging.Logger

import com.clark.spark.util.CommonUtils._
import com.clark.spark.util.Table
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.{SQLContext, SaveMode}

class AggregateIDFLFact extends Table {
  override def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {

    log.info(s"Processing Low Grain to AggregateIDFLFact for table : " )
    var adlsName = args(3)
    var adlsPath = getAdlsPath(adlsName)
    val aggregateIDFLFact = s"select aggregate_id, name, customer_id, birthdate, current_timestamp as etl_created_date, current_timestamp as etl_updated_date, 'clarkadmin' as etl_created_by, 'clarkadmin' as etl_updated_by, 'Nested_Json' as etl_source from read_json_flat_file"
    var fileList = 0
    try{
      fileList = dbutils.fs.ls(adlsPath + s"lg/aggregate_id_fl_fact").size
    }
    catch {
      case e: Exception => e.printStackTrace
        log.info("LG is not available")
    }

    if (fileList > 0) {
      ssqc.read.parquet(adlsPath + s"raw/nested_df").createOrReplaceTempView("read_json_parquet")

      ssqc.sql(aggregateIDFLFact).write.mode(SaveMode.Append).parquet(adlsPath + s"lg/aggregate_id_fl_fact")
      log.info("Raw to Low Grain processed for table aggregate_id_fl_fact to " + adlsPath + s"lg/aggregate_id_fl_fact")
    }
    else {
      ssqc.read.parquet(adlsPath + s"raw/nested_df").createOrReplaceTempView("read_json_parquet")

      ssqc.sql(aggregateIDFLFact).write.mode(SaveMode.Overwrite).parquet(adlsPath + s"lg/aggregate_id_fl_fact")
      log.info("Raw to Low Grain processed for table aggregate_id_fl_fact to " + adlsPath + s"lg/aggregate_id_fl_fact")

    }
  }
}

