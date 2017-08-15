package com.company.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by gouthamvidyapradhan on 16/08/2017.
  * Simple exercise to sum up total customer expense
  */
object CustomerData {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CustomerData")

    val line = sc.textFile("../SparkScala/src/resources/customer-orders.csv")

    line.map(parseLine).reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey()
      .map(x => ("Customer ID: " + x._2, "Total Amount Spent: " + x._1))
      .collect()
      .foreach(println)
  }

  def parseLine(line: String) = {
    val fields = line.trim.split(",")
    val customerId = fields(0)
    val amount = fields(2).toFloat
    (customerId, amount)
  }

}
