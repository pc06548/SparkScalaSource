import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object PopularMovie {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc: SparkContext = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines: RDD[String] = sc.textFile("../ml-100k/u.data")
    
    val movieIds: RDD[Int] = lines.map(x => x.split("\t")(1).toInt)

    val xyz: (Int, Int) = movieIds.map((_,1)).reduceByKey(_+_).map(_.swap).max()

    println(xyz)
  }
}
