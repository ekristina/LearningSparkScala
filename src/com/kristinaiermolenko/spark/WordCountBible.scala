package com.kristinaiermolenko.spark

import org.apache.log4j._
import org.apache.spark._


object WordCountBible {

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "WordCountBible")

    // Download Bible text from project Gutenberg
    // http://www.gutenberg.org/cache/epub/10/pg10.txt
    val bible_source = scala.io.Source.fromURL("http://www.gutenberg.org/cache/epub/10/pg10.txt").mkString.toLowerCase

    // skipping first lines about project gutenberg
    // starting with `The First Book of Moses:  Called Genesis` line

    val list = bible_source.split("\n").filter(_ != "\r").drop(12)
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.parallelize(list)

    // another way of splitting would be:
    // lines.flatMap(x => x.split("\\W+")) but then I lose
    val words = lines
      .map({line => line.replaceAll("[^a-zA-Z ']", "")})
      .flatMap(x => x.split(" "))

    val wordCounts = words
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)

    val wordCountsSorted = wordCounts.map(x => (x._2.toInt, x._1)).sortByKey()

    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2

      println(s"$word: $count")
    }

  }
}
