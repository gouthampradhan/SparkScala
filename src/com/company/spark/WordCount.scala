package com.company.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by gouthamvidyapradhan on 15/08/2017.
  */
object WordCount {
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCountBetter")

    val line = sc.textFile("../SparkScala/src/resources/book.txt")

    //The hard way
    line.flatMap(x => x.split("\\W+"))
      .map(x => x.toLowerCase)
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey()
      .map(x => (x._2, x._1))
      .collect()
      .foreach(println)

    //The easy and simple way
    //val count = line.flatMap(x => x.split("\\W+")).countByValue()

  }
}
