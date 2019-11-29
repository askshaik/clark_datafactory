package com.clark.spark.util


import com.clark.spark.dimensions._
import com.clark.spark.factlessfacts.AggregateIDFLFact
import com.clark.spark.factlessfacts._
import com.clark.spark.summary._

import scala.collection.mutable

object TableFactory {
  def apply(tableName: String): Table = {

    val tableMap = new mutable.HashMap[String, Table]()

    tableMap.put("CustomerDemographic", new CustomerDemographic)
    tableMap.put("AggregateIDFLFact", new AggregateIDFLFact)
    tableMap.put("IDFLFact", new IDFLFact)
    tableMap.put("OrderSpecificTimeSummary", new OrderSpecificTimeSummary)
    tableMap.put("ProbabilitySummary", new ProbabilitySummary)
    tableMap.put("TimeToFulfilOrderSummary", new TimeToFulfilOrderSummary)
    tableMap(tableName)
  }
}
