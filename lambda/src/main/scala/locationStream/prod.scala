package locationStream

import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * Created by k.neffati on 14/03/2017.
  */
object prod {
  def main(args: Array[String]): Unit = {

    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    println(producer.partitionsFor("userlog-text"))
    for (nEvents <- Range(0, 100)) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new ProducerRecord[String, String]("userlog-text", ip, msg)

      //async
      //producer.send(data, (m,e) => {})
      //sync
      producer.send(data)
    }

    System.out.println("sent per second: " + 100 * 1000 / (System.currentTimeMillis() - t))
    producer.close()
  }

}
