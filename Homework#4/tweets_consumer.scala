import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.spark.rdd.RDD
//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe



object WordCountTest {

  def main(args: Array[String]) {

    val checkpointDir = "./checkpoint"
    val topic = "tweets"
    val conf = new SparkConf()
      .setAppName("tweetsHashTagCounts")
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
     
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkpointDir)
    
    val topics = Set(topic)
    val kafkaParams = Map[String, Object](
                "bootstrap.servers" -> "kafka-headless:9092",
                "key.deserializer" -> classOf[StringDeserializer],
                "value.deserializer" -> classOf[StringDeserializer],
                "group.id" -> "tweets",
                "auto.offset.reset" -> "latest",
                "enable.auto.commit" -> (false: java.lang.Boolean)
                )
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
                    ssc,
                    PreferConsistent,
                    Subscribe[String, String](topics, kafkaParams)
                    )
    val counts = kafkaStream.map(_._1)\
                      .map(record=>record.split(",2021")._1) \
                      .flatMap(_.split(" ")) \
                      .filter(_.startswith("#")) \
                      .map((_,1)) \
                      .reduceByKeyAndWindow(_+_,_-_ ,Seconds(30),Seconds(10))

    stream.foreachRDD(rdd => {
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      sorted_rdd = rdd.sortBy(_._2,false)
      println(sorted_rdd.take(30))
    })

      //line.saveAsTextFiles(outputPath)

    ssc.start()
    ssc.awaitTermination()
  }

}