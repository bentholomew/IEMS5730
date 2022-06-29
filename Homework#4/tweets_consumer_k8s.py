from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import json
import time


def process_rdd(time, rdd):
    print("---------- %s -----------" % str(time))
    try:
        print(rdd.collect())
    except:
        e = sys.exc_info()
        print("Error: ", e)

if __name__ == '__main__':
    sc = SparkContext(appName="TweetsHashtagCount")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('./checkpoint')
    kafkaStream = KafkaUtils.createStream(ssc, 'kafka-headless:9092', 'consumer', {'tweets': 2})
    counts = kafkaStream.map(lambda x: x[1])
                    #    .map(lambda x:x.split(",2021")[0]) \
                    #    .flatMap(lambda x:x.split(" ")) \
                    #    .filter(lambda x: x[1]=='#') \
                    #    .map(lambda x:(x,1)) \
                    #    .reduceByKeyAndWindow(lambda x,y:x+y, lambda x,y: x-y, 30, 10) 
    counts.foreachRDD(process_rdd)
    ssc.start()
    ssc.awaitTermination()