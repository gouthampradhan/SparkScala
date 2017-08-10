package com.company.spark

import org.apache.spark.SparkContext

/**
  * Created by gouthamvidyapradhan on 10/08/2017.
  * Simple spark class to calculate the average number of friends by name
  */
object FriendsByName {

  def parseLine(line: String) = {
    val fields = line.trim().split(",")
    val name = fields(1)
    val numberOfFriends = fields(3).toInt
    (name, numberOfFriends)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "FriendsByName")
    val lines = sc.textFile("../SparkScala/src/resources/fakefriends.csv")
    lines.map(parseLine).mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)
      .collect()
      .sortBy(x => x._2) //sort by number of friends
      .foreach(println)
  }
}

