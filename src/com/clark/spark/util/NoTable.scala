package com.clark.spark.util

import java.util.logging.Logger

import org.apache.spark.sql.SQLContext

class NoTable extends Table {
  override def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {

    log.info("given table is not configured :" + args(0))
  }

}
