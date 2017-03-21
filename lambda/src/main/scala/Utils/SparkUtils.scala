package Utils

import java.lang.management.ManagementFactory

import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by k.neffati on 13/03/2017.
  */
object SparkUtils {
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkSession(appName: String) = {

    var checkpointDirectory = ""

    if (isIDE) {
      System.setProperty("hadoop.home.dir", "E:\\hadoop") // required for winutils
      checkpointDirectory = "file:///E:/fileoutput/temp"
    }
    else {
      checkpointDirectory = "hdfs://sandbox:9000/spark/checkpoint"
    }


    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(appName)
      .config("spark.cassandra.connection.host", "cassandra")
      .getOrCreate()

    spark
  }

  def getSQLContext(ss: SparkSession) = {
    val sqlContext = ss.sqlContext
    sqlContext
  }

  //getStreamingContext allow us to recreate a StreamingContext from last checkpoint if it's available and if it's not available it get an active StreamingContext or create one
  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext, ss: SparkSession, batchDuration: Duration) = {
    val sparkContext = ss.sparkContext
    //streamingApp is a function that create a new streaming context
    val creatingFunc = () => streamingApp(sparkContext, batchDuration)
    val ssc = sparkContext.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sparkContext.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    //set the check point directory if it exist
    sparkContext.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }


  def getKafkaDirectStream(ssc: StreamingContext, topic: String, broker: String)= {
    val kafkaDirectParams = Map(
      "bootstrap.servers" -> broker,
      "group.id" -> "lambda",
      "auto.offset.reset" -> "largest"
    )

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaDirectParams, Set(topic)
    )
    kafkaDirectStream
  }


}


























