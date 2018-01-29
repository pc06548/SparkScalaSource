import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object PriceByCustomer {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc: SparkContext = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines: RDD[String] = sc.textFile("../customer-orders.csv")

    val customersData: RDD[(Int, Float)] = lines.map(x => {
      val data = x.split(",")
      (data(0).toInt, data(2).toFloat)
    })

    val result: RDD[(Int, Float)] = customersData.reduceByKey((x, y) => x+y)

    result.map(_.swap).sortByKey().collect().foreach(println(_))
  }
}
