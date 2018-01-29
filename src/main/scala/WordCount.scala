import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object WordCount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc: SparkContext = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines: RDD[String] = sc.textFile("../book.txt")

    val lowerCasedWords: RDD[(String, Int)] = lines.flatMap(x => x.split("\\W+")).map(_.toLowerCase).map(x => (x,1))

    val lowerCaseWithCount: Array[(Int, String)] = lowerCasedWords.reduceByKey(_+_).map(x => x.swap).sortByKey().collect()

    lowerCaseWithCount.foreach(println(_))


  }
}
