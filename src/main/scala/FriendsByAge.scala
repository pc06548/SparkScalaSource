import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FriendsByAge {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "FriendsByAge")
    val friends: RDD[String] = sc.textFile("../fakefriends.csv")
    val ageFriendsPair: RDD[(Int, Int)] = friends.map(x => {
      val friendsData = x.split(",")
      (friendsData(2).toInt, friendsData(3).toInt)
    })

    val groupedAgeFPair: RDD[(Int, Iterable[Int])] = ageFriendsPair.groupByKey()

    val friendsByAvgAge: RDD[(Int, Int)] = groupedAgeFPair.mapValues(values => values.sum/values.size)

    val result =friendsByAvgAge.collect()

    result.sorted.foreach(println(_))

  }

}
