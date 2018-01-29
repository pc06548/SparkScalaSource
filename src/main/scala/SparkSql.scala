import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkSql {

  case class Person(id:Int, name: String, age: Int, numberOfFriends: Int)
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSql").master("local[*]")
      .config("spark.sql.warehouse.dir","file:///C:/temp").getOrCreate()

    import sparkSession.implicits._
    val sparkContext = sparkSession.sparkContext

    val lines = sparkContext.textFile("../fakefriends.csv")

    val people: RDD[Person] = lines.map(line => {
      val fields = line.split(",")
      Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    })

    val peopleDataSet: Dataset[Person] = sparkSession.createDataset(people)

    peopleDataSet.printSchema()

    peopleDataSet.createOrReplaceTempView("people")

    val teenagers = sparkSession.sql("select * from people where age >= 13 and age <=19")

    teenagers.collect().map(println(_))

    sparkSession.stop()

  }
}
