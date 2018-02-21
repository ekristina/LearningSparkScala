package hello_world

import org.apache.spark._
import org.apache.log4j._
import scala.math.max


object Precipitation {
  
  def parseLine(line:String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val precip = fields(3).toFloat
    (stationID, entryType, precip)
  }

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Precipitation")
    
    val lines = sc.textFile("../1800.csv")
    val parsedLines = lines.map(parseLine)
    val precips = parsedLines.filter(x => x._2 == "PRCP")
    val stationPrecip = precips.map(x => (x._1, x._3.toFloat))
    val maxPrecipByStation = stationPrecip.reduceByKey( (x,y) => max(x,y))
    val results = maxPrecipByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val precip = result._2
       val formattedPrecip = f"$precip%.2f"
       println(s"$station max precipitation: $formattedPrecip") 
    }
      
  }
}