package MovieRecommendation

import org.apache.log4j._
import org.apache.spark.SparkContext

import scala.io.Source

object PopularMoviesNameMapping {

  def loadMovieNames(): Map[Int, String] = {

    // Create a Map of Ints to Strings and populate it from movies.csv

    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("./ml-latest/movies.csv").getLines().drop(1) // drop header

    for (line <- lines) {
      // check if line is not empty
      if (line.length != 0) {

        val fields = line.split(",")
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    movieNames

  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularMoviesNameMapping")

    val movieNames = sc.broadcast(loadMovieNames()) // broadcasts on every node of the cluster

    val lines = sc.textFile("./ml-latest/ratings.csv")

    // Count how much lines of each movie - means how popular it is among users

    // drop header from rdd
    val dropHeader = lines.mapPartitionsWithIndex((ind, iter) => if (ind == 0) iter.drop(1) else iter)

    val movies = dropHeader.map(x => x.split(",")(1).toInt)

    val moviesCount = movies.map(x => (x, 1))
    val reducedMovies = moviesCount.reduceByKey((x, y) => x + y)
    val moviesWithNames = reducedMovies.map(x => (movieNames.value(x._1), x._2))

    val sortedMovies = moviesWithNames.map(x => (x._2, x._1)).sortByKey(ascending = false)

    val results = sortedMovies.top(20)

    results.foreach(println)

  }

}
