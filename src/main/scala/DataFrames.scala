import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object DataFrames {

  case class Person(id:Int, name: String, age: Int, numberOfFriends: Int)
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSql").master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp").getOrCreate()

    import sparkSession.implicits._
    val sparkContext = sparkSession.sparkContext

    val lines = sparkContext.textFile("../fakefriends.csv")

    val people: RDD[Person] = lines.map(line => {
      val fields = line.split(",")
      Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    })

    val peopleDs: Dataset[Person] = people.toDS().cache()

    peopleDs.select("name").show()

    println("People less than 21")
    peopleDs.filter(peopleDs("age") < 21).show()

    println("Group by age")
    peopleDs.orderBy("age").groupBy("age").count().show()



  }
}
