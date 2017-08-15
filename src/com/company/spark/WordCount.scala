package com.company.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by gouthamvidyapradhan on 15/08/2017.
  * Word count
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
      .filter(stopWords) //filter by common stop words
      .map(x => (x, 1)) // map each word to 1
      .reduceByKey((x, y) => x + y) //sum up the values by each key
      .map(x => (x._2, x._1)) //swap pairs
      .sortByKey() //sort by key
      .map(x => (x._2, x._1)) //swap back
      .collect() //collect and print
      .foreach(println)

    //The easy and simple way
    //val count = line.flatMap(x => x.split("\\W+")).countByValue()

  }

  /**
    * Common stop words to be used as filter
    * @param word
    * @return
    */
  def stopWords(word: String) = {
    word != "is" && word != "and" && word != "the" && word != "to" && word != "for" && word != "it" && word != "in"
  }

}
