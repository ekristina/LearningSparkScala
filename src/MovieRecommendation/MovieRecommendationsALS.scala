package MovieRecommendation

import org.apache.spark._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.mllib.recommendation._

object MovieRecommendationsALS {

  /* Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from ratings.csv
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("./ml-latest-small/movies.csv").getLines().drop(1)
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

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieRecommendationsALS")

    println("Loading movie names...")
    val nameDict = loadMovieNames()

    val dataWithHeader = sc.textFile("./ml-latest-small/ratings.csv")
    val data = dataWithHeader.mapPartitionsWithIndex((ind, iter) => if (ind == 0) iter.drop(1) else iter)

    val ratings = data.map( x => x.split(',') ).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble)).cache()

    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")

    val rank = 8
    val numIterations = 10

    val model = ALS.train(ratings, rank, numIterations)

    val userID = 1

    println("\nRatings for user ID " + userID + ":")

    val userRatings = ratings.filter(x => x.user == userID)

    val myRatings = userRatings.collect()

    for (rating <- myRatings) {
      println(nameDict(rating.product.toInt) + ": " + rating.rating.toString)
    }

    println("\nTop 10 recommendations:")

    val recommendations = model.recommendProducts(userID, 10)
    for (recommendation <- recommendations) {
      println( nameDict(recommendation.product.toInt) + " score " + recommendation.rating )
    }

  }
}