/**
 * Created by Administrator on 1/22/2015.
 */
import java.util.Properties
import kafka.producer._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    produceKafkaData(args)
  }
  def originProducer(args: Array[String]): Unit = {
    produceKafkaData(args)
  }

  def produceKafkaData(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage> <inputLocalFilePath>")
      System.exit(1)
    }
    val Array(brokers, topic, messagesPerSec, wordsPerMessage, localFilePath) = args
    // Input Data
    import scala.io.Source
    val data =  Source.fromFile(localFilePath).getLines.map(r => {
      new KeyedMessage[String, String](topic, r)
    }).toArray
    // Zookeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Send some messagescd ..
    while(true) {
      producer.send(data: _*)
//      data.foreach(r => {
//        println(r)
//      })
//      val messages = (1 to messagesPerSec.toInt).map { messageNum =>
//        val str = (1 to 3).map(x => scala.util.Random.nextInt(10).toString)
//          .mkString("\t")
//        new KeyedMessage[String, String](topic, str)
//      }.toArray
      //producer.send(messages: _*)
      Thread.sleep(100)
    }
  }

  def originProduceKafka(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Send some messages
    while(true) {
      val messages = (1 to messagesPerSec.toInt).map { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")
        new KeyedMessage[String, String](topic, str)
      }.toArray

      producer.send(messages: _*)
      Thread.sleep(100)
    }
  }
}
