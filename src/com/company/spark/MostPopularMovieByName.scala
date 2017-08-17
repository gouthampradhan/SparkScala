package com.company.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

/**
  * Created by gouthamvidyapradhan on 17/08/2017.
  * Most popular movie based on number of ratings received.
  */
object MostPopularMovieByName {

  def loadMovieName() : Map[Int, String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames : Map[Int, String] = Map()

    val lines = Source.fromFile("../SparkScala/src/resources/ml-100k/u.item").getLines()
    for(line <- lines){
      val fields = line.split('|')
      if(fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    movieNames
  }


  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularMovieByName")

    val nameDict = sc.broadcast(loadMovieName())

    val lines = sc.textFile("../SparkScala/src/resources/ml-100k/u.data")

    lines.map(x => x.split("\t")(1)).countByValue()
        .toSeq
        .sortBy(_._2)
        .reverse
        .map(x => (nameDict.value(x._1.toInt), x._2))
        .foreach(println)
  }

}
