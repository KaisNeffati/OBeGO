package StreamingProcessing

import Entities.User
import Utils.SparkUtils._
import config.Settings
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.HasOffsetRanges

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

/**
  * Created by k.neffati on 12/03/2017.
  */
object Streaming {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val ss = getSparkSession("Spark Streaming Receiver")
    val sc = ss.sparkContext
    val sqlContext = getSQLContext(ss)
    val batchDuration = Seconds(5)

    //to use toDF() function
    import sqlContext.implicits._

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val wlc = Settings.WeblogGen
      val topic = wlc.kafkatopic

      val kafkaDirectStream = getKafkaDirectStream(ssc, topic, "kafka:9092")

      val userStream = kafkaDirectStream.transform(input => {
        val offserRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges
        input.mapPartitionsWithIndex({ (index, it) =>
          val or = offserRanges(index)
          it.flatMap { kv =>
            val line = kv._2
            val record = line.split("\\t")
            val MS_IN_HOUR = 1000 * 60 * 60
            if (record.length == 6) {
              Some(User(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3) + " " + record(4), record(5)))
            }
            else {
              None
            }
          }
        })
      }).cache()
      userStream.print()

      userStream.transform { rdd =>
        val df=rdd.toDF()
        df.createOrReplaceTempView("users")
        val usersByPlace = sqlContext.sql(
          """
            |SELECT Count(place) as persons_by_place, place
            |FROM users
            |GROUP by place
          """.stripMargin)
        usersByPlace.rdd.saveToCassandra("lambda","stream_users_by_places")
        val u = usersByPlace.select("persons_by_place", "place")
        u.rdd
      }.print()

      ssc
    }

    val ssc = streamingApp(sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()
  }
}
