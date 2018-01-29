import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.io.Source

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object PopularSuperHero {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc: SparkContext = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines: RDD[String] = sc.textFile("../Marvel-graph.txt")

    val networkGraph: RDD[Int] = lines.flatMap(x => {
      val data = x.split("\\s+")
      data.tail.map(_.toInt)
    })

    val zzz = networkGraph.map((_,1)).reduceByKey(_+_).map(_.swap).max




    val superHeroIdToName: RDD[(Int, String)] = sc.textFile("../Marvel-names.txt").map(x => {
      val data = x.split(""""""")
      (data.head.trim.toInt, data.last)
    })
    superHeroIdToName.lookup(zzz._2).map(println(_))
    superHeroIdToName.filter(_._1 == zzz._2).collect().map(println(_))


  }
}
