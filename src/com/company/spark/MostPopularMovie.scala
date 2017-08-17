package com.company.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by gouthamvidyapradhan on 17/08/2017.
  */
object MostPopularMovie {
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularMovie")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../SparkScala/src/resources/ml-100k/u.data")

    //print the combination of (Movie_ID, RatingCount)
    lines.map(x => x.split("\t")(1)).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
  }
}
