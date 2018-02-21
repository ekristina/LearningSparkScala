package hello_world

import org.apache.log4j._
import org.apache.spark._

/*
TOP 20 words:

go: 1492
let: 1511
men: 1653
land: 1718
day: 1734
also: 1769
children: 1802
one: 1967
come: 1971
house: 2024
came: 2093
people: 2139
king: 2257
son: 2370
israel: 2565
man: 2613
upon: 2748
said: 3999
god: 4442
lord: 7830

 */


object WordCountBible {

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "WordCountBible")

    // Download Bible text from project Gutenberg
    // http://www.gutenberg.org/cache/epub/10/pg10.txt
    val bible_source = scala.io.Source.fromURL("http://www.gutenberg.org/files/10/10.txt").mkString
    // skipping first and last lines about project gutenberg
    // starting with `The First Book of Moses:  Called Genesis` line
    val list = bible_source.split("\n").slice(37, 99850)
    val lines = sc.parallelize(list)

    // OR
    // val lines = sc.textFile("./bible_kj.txt")

    val stopwords = scala.io.Source.fromFile("./english_stop_words.txt").getLines().toList

    // another way of splitting would be:
    // lines.flatMap(x => x.split("\\W+"))
    val words = lines
      .map({line => line.replaceAll("[^a-zA-Z ]", "").toLowerCase()})
      .flatMap(x => x.split(" ")).filter(!stopwords.contains(_))

    val wordCounts = words
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)

    val wordCountsSorted = wordCounts.map(x => (x._2.toInt, x._1)).sortByKey()


    println("| word | count |\n" +
            "|------|-------|")
    for (result <- wordCountsSorted.top(20)) {
      val count = result._1
      val word = result._2

      println(s"|$word | $count|")
    }

  }
}
