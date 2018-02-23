package MovieRecommendation

import org.apache.log4j._
import org.apache.spark.SparkContext

object PopularMovies {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularMovies")

    val lines = sc.textFile("./ml-latest/ratings.csv")

    // Count how much lines of each movie - means how popular it is among users

    // drop header from rdd
    val dropHeader = lines.mapPartitionsWithIndex((ind, iter) => if (ind == 0) iter.drop(1) else iter)

    val movies = dropHeader.map(x => x.split(",")(1).toInt)

    val moviesCount = movies.map(x => (x, 1))
    val reducedMovies = moviesCount.reduceByKey((x, y) => x + y)

    val sortedMovies = reducedMovies.map(x => (x._2, x._1)).sortByKey(ascending = false)

    val results = sortedMovies.top(20)
    results.foreach(println)
  }

}
