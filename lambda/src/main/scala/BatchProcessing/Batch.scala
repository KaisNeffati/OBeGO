package BatchProcessing

import java.lang.management.ManagementFactory

import Entities.User
import org.apache.spark.sql.{SaveMode, SparkSession}
import Utils.SparkUtils._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import config.Settings

/**
  * Created by k.neffati on 07/03/2017.
  */
object Batch {

  def main(args: Array[String]): Unit = {


    val spark = getSparkSession("Spark Batch Job")
    val wlc =Settings.WeblogGen


    val userDF=spark.read.parquet(wlc.hdfsPath)
      .where("unix_timestamp()- timestamp_hour /1000 <=60*60*8")

    userDF.createOrReplaceTempView("users")
    val sql =userDF.sqlContext.sql(
      """
        |SELECT Count(place) as persons_by_place, place
        |FROM users
        |GROUP by place
      """.stripMargin)
      sql.show()

    sql.write.format("org.apache.spark.sql.cassandra")
      .options(Map( "keyspace" -> "lambda", "table" -> "batch_users_by_places"))
      .save()
  }
}
