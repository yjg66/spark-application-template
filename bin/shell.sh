bin/run-example org.apache.spark.examples.streaming.KafkaWordCountProducer 10.190.172.43:9092 test 3 5
bin/spark-submit   --class "KafkaProducer"   --master spark://10.190.172.43:7077   /mnt/ssd/dm/kafka/spark-application-template/target/scala-2.10/benchmark-breeze-on-spark-assembly-0.0.1.jar 10.190.172.43:9092 test2 3 5 /mnt/ssd/dm/kafka/spark-application-template/src/resource/percentileData.txt

bin/spark-submit   --class "KafkaTest"   --master spark://10.190.172.43:7077   /mnt/ssd/dm/kafka/spark-application-template/target/scala-2.10/benchmark-breeze-on-spark-assembly-0.0.1.jar 10.190.172.43:9092 test2 3 5 hdfs://10.190.172.89:8020/yanjiedata/kafkaOutput1

./bin/spark-submit \
  --class <main-class>
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]