package MovieRecommendation

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}
import scala.math.sqrt

object MovieSimilarities {

  /* Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from movies.csv.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("./ml-latest/movies.csv").getLines().drop(1)

    for (line <- lines) {
      if (line.length != 0) {

        if (line.contains("\"")) {
          val fields = line.split("\"")
          fields(0) = fields(0).replace(",", "")
          movieNames += (fields(0).toInt -> fields(1))
        }

        else {
          val fields = line.split(",")
          movieNames += (fields(0).toInt -> fields(1))
        }

      }
     }

     movieNames
  }

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))


  def makePairs(userRatings:UserRatingPair): ((Int, Int), (Double, Double)) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    movie1 < movie2
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    (score, numPairs)
  }

  def computePearsonCorr(ratingPairs: RatingPairs): (Double, Int) = {
    val numPairs:Int = ratingPairs.size

    var sumX:Double = 0.0
    var sumY:Double = 0.0
    var nominator:Double = 0.0

    var xSum: Double = 0.0
    var ySum: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sumX += ratingX
      sumY += ratingY

    }
    val meanX = sumX/numPairs
    val meanY = sumY/numPairs

    for (pair <- ratingPairs) {
      val x: Double = pair._1
      val y: Double = pair._2

      nominator += (x - meanX) * (y - meanY)
      xSum += (x - meanX) * (x - meanX)
      ySum += (y - meanY) * (y - meanY)
    }

    var corr:Double = 0.0

    val denominator:Double = sqrt(xSum) * sqrt(ySum)
    if (denominator != 0) {
      corr = nominator / denominator
    }

    (corr, numPairs)
  }


  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieSimilarities")

    println("\nLoading movie names...")
    val nameDict = loadMovieNames()

    val dataWithHeader = sc.textFile("./ml-latest-small/ratings.csv")
    val data = dataWithHeader.mapPartitionsWithIndex((ind, iter) => if (ind == 0) iter.drop(1) else iter)

    // Map ratings to key / value pairs: user ID => movie ID, rating
    val ratings = data.map(l => l.split(",")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings = ratings.join(ratings)

    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey()
    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    //Save the results if desired
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")

    // Extract similarities for the movie we care about that are "good".

//    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0

      // val movieID:Int = args(0).toInt  // if using terminal
      val movieID = 2324 // testing Life is Beautiful
      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above

      val filteredResults = moviePairSimilarities.filter( x =>
        {
          val pair = x._1
          val sim = x._2
          (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        }
      )

      // Sort by quality score.
      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(ascending = false).take(10)

      println("\nTop 10 similar movies for " + nameDict(movieID) + "\n")

      println("|Movie|score|strength|\n" +
      "|-----|-------|---------|")
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(s"|${nameDict(similarMovieID)}|${sim._1}|${sim._2}|\n")
//      }
    }
  }
}
