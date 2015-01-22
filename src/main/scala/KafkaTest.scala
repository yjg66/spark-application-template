import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 1/22/2015.
 */
object KafkaTest {
  def main(args: Array[String]): Unit = {
    testKafka()
  }

  def testKafka(): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val sc = new SparkContext(sparkConf)
    import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
    import StreamingContext._
    //import org.apache.spark.streaming.kafka.KafkaUtils
    import java.util.Properties
    import org.apache.spark.streaming._
    import org.apache.spark.SparkConf
    val Array(zkQuorum, group, topics, numThreads) = Array("10.190.172.43:2181", "test-consumer-group", "test", "1")
    val ssc =  new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
