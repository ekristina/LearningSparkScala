package hello_world

/*
*
* Solving homework with customer-orders.csv
*
*/

import org.apache.log4j._
import org.apache.spark._


object CustomerOrders {

  def parseLines(line: String): (Int, Float) = {

    val lineList = line.split(",")

    val customer_id = lineList(0).toInt
    val amount_spent = lineList(2).toFloat

    (customer_id, amount_spent)
  }

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "CustomerOrders")

    val lines = sc.textFile("./customer-orders.csv")

    val parsedLines = lines.map(parseLines)

    val mapped = parsedLines.map(x => (x._1, x._2))
      .reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey()

    for (result <- mapped) {

      val customer = result._2
      val amount = result._1

      println(s"Customer $customer spent $amount")

    }


  }
}
