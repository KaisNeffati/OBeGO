package config

import com.typesafe.config.ConfigFactory

/**
  * Created by k.neffati on 07/03/2017.
  */
object Settings {
  private val conf = ConfigFactory.load();

  object WeblogGen {
    lazy val weblogGen = conf.getConfig("locationStream")
    lazy val records = weblogGen.getInt("records")
    lazy val timeMultiplier = weblogGen.getInt("time_multiplier")
    lazy val location = weblogGen.getInt("location")
    lazy val persons = weblogGen.getInt("persons")
    lazy val filePath = weblogGen.getString("file_path")
    lazy val destPath = weblogGen.getString("dest_path")
    lazy val numberOfFiles = weblogGen.getInt("number_of_files")
    lazy val kafkatopic=weblogGen.getString("kafka_topic")
    lazy val hdfsPath=weblogGen.getString("hdfs_path")


  }

}
