package com.clark.spark.util

import com.clark.spark.aggregates._
import com.clark.spark.dimensions._
import com.clark.spark.facts._
import com.clark.spark.misc._
import com.clark.spark.summary._
import scala.collection.mutable

object TableFactory {
  def apply(tableName: String): Table = {

    val tableMap = new mutable.HashMap[String, Table]()

    tableMap.put("CustomerDemographic", new CustomerDemographic)
    tableMap(tableName)
  }
}
