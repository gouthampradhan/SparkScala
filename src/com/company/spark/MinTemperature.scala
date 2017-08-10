package com.company.spark

import org.apache.spark.SparkContext

/**
  * Created by gouthamvidyapradhan on 11/08/2017.
  * Calculate min temperature for each station
  */
object MinTemperature {

  def parseLine(line: String) = {
    val fields = line.trim.split(",")
    val station = fields(0)
    val indicator = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (station, indicator, temperature)
  }


  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "MinTemperature")
    val lines = sc.textFile("../SparkScala/src/resources/1800.csv")
    lines.map(parseLine)
      .filter(x => x._2 == "TMIN")
      .map(x => (x._1, x._3.toFloat))
      .reduceByKey((x, y) => math.min(x, y))
      .map(x => ("Station_ID= " + x._1, " Min temperature= " + x._2 + " F"))
      .collect()
      .foreach(println)
  }
}
