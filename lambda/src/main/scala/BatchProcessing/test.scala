package BatchProcessing

import org.apache.spark.sql.SparkSession

/**
  * Created by k.neffati on 12/03/2017.
  */
object test {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .getOrCreate()

  println(spark.version)
  }
}
