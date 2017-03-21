package StreamingProcessing

import Entities.User
import Utils.SparkUtils._
import config.Settings
import org.apache.spark._
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
  * Created by k.neffati on 12/03/2017.
  */
object StreamingSaveToHDFS {
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
      val hdfsPath =wlc.hdfsPath


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
              Some(User(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3) + " " + record(4), record(5),
                Map("topic" -> or.topic.toString, "kafkaPartition" -> or.partition.toString, "fromOffset" -> or.fromOffset.toString, "untilOffset" -> or.untilOffset.toString)))
            }
            else {
              None
            }
          }
        })
      }).cache()
      userStream.print()


      userStream.foreachRDD { rdd =>
        val userDF = rdd
          .toDF()
          .selectExpr("timestamp_hour", "place", "location", "person", "interst", "inputPropos.topic as topic", "inputPropos.kafkaPartition as kafkaPartition", "inputPropos.fromOffset as fromOffset", "inputPropos.untilOffset as untilOffset")

        userDF
          .write
          .partitionBy("topic", "kafkaPartition", "timestamp_hour")
          .mode(SaveMode.Append)
          .parquet(hdfsPath)
      }

      ssc
    }

    val ssc = streamingApp(sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()
  }
}
