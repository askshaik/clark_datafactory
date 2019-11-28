package com.clark.spark.util

import java.util.logging.Logger

import org.apache.spark.sql.SQLContext

trait Table {
  def processRawToLowGrain(args: Array[String], ssqc: SQLContext, log: Logger): Unit = {}

}
