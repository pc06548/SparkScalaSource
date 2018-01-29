import scala.math._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MinTemperatures {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "MinTemperatures")
    val weather: RDD[String] = sc.textFile("../1800.csv")
    val weatherMappedData: RDD[(String, String, Float)] = weather.map(x => {
      val weatherData: Array[String] = x.split(",")
      (weatherData(0), weatherData(2), weatherData(3).toFloat * 0.1f * (9.0f/5.0f) + 32.0f)
    })

    val tminData: RDD[(String, Float)] = weatherMappedData.filter(_._2 == "TMIN").map(x => (x._1, x._3))

    val minResult: RDD[(String, Float)] = tminData.reduceByKey(min)

    val result = minResult.collect()

    result.sorted.foreach(println(_))

  }

}
