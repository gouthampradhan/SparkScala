package com.company.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by gouthamvidyapradhan on 23/08/2017.
  */
object MostPopularSuperHero {

  def countChildren(line: String) = {
    val elements = line.split("\\s+");
    (elements(0).toInt, elements.length - 1)
  }


  def parseName(line: String) : Option[(Int, String)] = {
    val fields = line.split('\"')
    if(fields.length > 1){
      Some(fields(0).trim().toInt, fields(1).trim())
    } else{
      None
    }
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularSuperHero")

    val superheros = sc.textFile("../SparkScala/src/resources/Marvel-graph.txt")
    val graph = superheros.map(countChildren)

    val names = sc.textFile("../SparkScala/src/resources/Marvel-names.txt")
                  .flatMap(parseName)

    val mostPopular = graph.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).max()

    val mostPopularSuperHero = names.lookup(mostPopular._2)(0)

    println(s"$mostPopularSuperHero is the most popular super hero with ${mostPopular._1} connections")

  }
}

