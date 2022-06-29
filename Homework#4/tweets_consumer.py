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
        # print(rdd.top(5,key=lambda x: x[1])) # Report Error Py4JJavaError(u'An error occurred while calling z:org.apache.spark.api.python.PythonRDD.collectAndServe.\
        sorted_rdd = rdd.sortBy(lambda x:x[1],ascending=False)
        print(sorted_rdd.take(30)) # Check output @ yarn-webUI/applicationID/log/stdout
    except:
        e = sys.exc_info()
        print("Error: ", e)

if __name__ == '__main__':
    sc = SparkContext(appName="TweetsHashtagCount")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('./checkpoint')
    kafkaStream = KafkaUtils.createStream(ssc, 'dicvmd7.ie.cuhk.edu.hk:2181', 'consumer', {'1155162635_hw4': 2})
    counts = kafkaStream.map(lambda x: x[1])\
                       .map(lambda x:x.split(",2021")[0]) \
                       .flatMap(lambda x:x.split(" ")) \
                       .filter(lambda x: x.startswith("#")) \
                       .map(lambda x:(x,1)) \
                       .reduceByKeyAndWindow(lambda x,y:x+y, lambda x,y: x-y, 300, 120) # Window Length: 5min // TimeInterval: 2min 
    counts.foreachRDD(process_rdd)
    ssc.start()
    ssc.awaitTermination()