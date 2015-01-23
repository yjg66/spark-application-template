
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Created by Administrator on 1/22/2015.
 */
object KafkaTest {
  def main(args: Array[String]): Unit = {
    //testKafka()
    agg(args)
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
  // run this use
  // bin/spark-submit
  // --class "KafkaTest"
  // --master spark://10.190.172.43:7077
  // /mnt/ssd/dm/kafka/spark-application-template/target/scala-2.10/benchmark-breeze-on-spark-assembly-0.0.1.jar
  def agg(args: Array[String]): Unit ={
    val conf = new SparkConf
    val sc = new SparkContext(conf)
    //val ssc = new StreamingContext(conf, Seconds(2))
    //val ssc = new OLA2_StreamingContext(sc, null, Seconds(conf.get("ola.common.sparkstreaming_batch_seconds").toInt))
    val timeDuration = Seconds(4)
    import StreamingContext._
    // Array("10.190.172.43:2181", "test-consumer-group", "test", "1")
    val Array(zkQuorum, group, topics, numThreads, outPutHdfsPath) = Array(args(0), args(1), args(2), args(3), args(4))
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val ssc =  new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")
    val text = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
   // val text = ssc.textFileStream("hdfs://10.172.98.79:9000/StreamingSample_small700MB.txt")
    //val text = ssc.OLA2_textFileStream("hdfs://10.172.98.79:9000/StreamingSample_small700MB.txt", 2, false, false)
    val result = text.map(record => {
      // parse the data
      //val s = record.split("\t")
      val key = Seq(record, record)
        //Seq(s(0), s(1))
      val value = (System.currentTimeMillis() % 1000).toInt
     //record(2).toInt

          //s(2).toInt
      (key, value)
    }).mapPartitions(iter => {
      // map Side aggregate the results
      // HashMap is Map(key -> Map(value, count))
      val resultMap = new HashMap[Seq[String], HashMap[Int, Int]]
      var tmp:(Seq[String], Int) = null
      while(iter.hasNext) {
        tmp = iter.next()
        val valueMap = resultMap.getOrElse(tmp._1, new HashMap[Int, Int])
        var count = valueMap.getOrElse(tmp._2, 0)
        valueMap.put(tmp._2, count + 1)
        resultMap.put(tmp._1, valueMap)
      }
      resultMap.iterator
    }).reduceByKeyAndWindow((x: HashMap[Int, Int], y: HashMap[Int, Int]) => {
      // reduce side aggregate the results
      // combine the 2 HashMap
      y.foreach(r => {
        x.put(r._1, x.getOrElse(r._1, 0) + r._2)
      })
      x
    } , timeDuration, timeDuration).mapPartitions(iter => {
      // compute the percentage
      // result Map(key, Map(percentage, value))
      val resultMap = new mutable.HashMap[Seq[String], mutable.HashMap[Double, Int]]()
      while(iter.hasNext) {
        val tmp = iter.next
        // compute the jump percentage key value
        val map = tmp._2
        val sumCount = map.map(r => r._2).reduce(_+_)
        val p25 = sumCount * 0.25
        val p50 = sumCount * 0.5
        val p70 = sumCount * 0.75
        val sortDataSeq = map.toSeq.sortBy(r => r._1)
        val iterS = sortDataSeq.iterator
        var curTmpSum = 0.0
        var prevTmpSum = 0.0
        val valueMap = new mutable.HashMap[Double, Int]()
        while(iterS.hasNext) {
          val tmpData = iterS.next()
          prevTmpSum = curTmpSum
          curTmpSum += tmpData._2
          if(prevTmpSum <= p25 && curTmpSum >= p25) {
            valueMap.put(0.25, tmpData._1)
          } else if(prevTmpSum <= p50 && curTmpSum >= p50) {
            valueMap.put(0.5, tmpData._1)
          } else if(prevTmpSum <= p70 && curTmpSum >= p50) {
            valueMap.put(0.75, tmpData._1)
          }
        }
        resultMap.put(tmp._1, valueMap)
      }
      resultMap.iterator
    })
    result.print
    //result.saveAsTextFiles(outPutHdfsPath)
    ssc.start()
    ssc.awaitTermination()
  }
}
