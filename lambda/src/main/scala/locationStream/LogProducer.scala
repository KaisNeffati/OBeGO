package locationStream

import java.util.Properties

import config.Settings
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

import scala.util.Random

/**
  * Created by k.neffati on 07/03/2017.
  */
object LogProducer extends App {

  val wlc = Settings.WeblogGen
  val Persons = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/Persons.csv")).getLines().toArray
  val Places = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/Places.csv")).getLines().toArray
  val locations = (1 to wlc.location).map("location-" + _)
  val Intrest = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/interests.csv")).getLines().toArray


  val rnd = new Random()

  val topic = wlc.kafkatopic

  val props = new Properties()

  props.put("bootstrap.servers", "kafka:9092")
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  /*
    val filePath = isIDE match {
      case true => wlc.filePath
      case false => "file:///fileoutput/data.tsv"
    }
    val destPath = isIDE match {
      case true => wlc.destPath
      case false => "file:///fileoutput/input/"
    }
  */

  for (fileCount <- 1 to wlc.numberOfFiles) {

    //val fw = new FileWriter(filePath, true)

    val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp


    for (iteration <- 1 to wlc.records) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
      timestamp = System.currentTimeMillis()
      val place = Places(rnd.nextInt(Places.length - 1))
      val person = Persons(rnd.nextInt(Persons.length - 1))
      val location = locations(rnd.nextInt(locations.length - 1))
      val interst = Intrest(rnd.nextInt(Intrest.length - 1))

      val line = s"$adjustedTimestamp\t$place\t$location\t$person\t$interst\n"
      val producerRecord = new ProducerRecord(topic, line)
      kafkaProducer.send(producerRecord)

      //fw.write(line)
      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }
    }


    //fw.close()
    /*
        val outputFile = FileUtils.getFile(s"${destPath}data_$timestamp")
        println(s"Moving produced data to $outputFile")
        FileUtils.moveFile(FileUtils.getFile(filePath),outputFile)
    */
    val sleeping = 2000
    println(s"Sleeping for $sleeping")

  }
  kafkaProducer.close()

}
