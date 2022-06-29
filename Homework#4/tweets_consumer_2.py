from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("tweets_consumer_structured") \
        .getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "http://dicvmd7.ie.cuhk.edu.hk:6667") \
        .option("subscribe", "1155162635_hw4") \
        .load()

    lines = df.selectExpr("CAST(timestamp AS TIMESTAMP)","CAST(value AS STRING)")
    words = lines.withColumn("contents",split('value', ",2021")) \
                    .select(col("contents")[0].alias("contents"),'timestamp')\
                    .withColumn("words",explode(split('contents',' ')))
    hashtags = words.filter(words.words.startswith("#"))
    hashtags_cnt = hashtags.withWatermark('timestamp','5 minutes')\
                           .groupBy(
                                window('timestamp',"5 minutes", "2 minutes"),
                                hashtags.words)\
                           .count().orderBy([asc('window'),desc('count')])
    # Starts streaming and save the results into memory 
    query = hashtags_cnt.writeStream \
                        .queryName("hashtag_cnts") \
                        .outputMode("complete") \
                        .format("memory") \
                        .trigger(processingTime='5 minutes')\
                        .start()    
    # Print the output at a 2-minute interval, same as the sliding interval
    while True:
        time.sleep(120)
        hashtags_rank = spark.sql("SELECT * FROM hashtag_cnts") \
                  .withColumn("rank",row_number().over(Window.partitionBy('window').orderBy(desc("count")))) 
        hashtags_top = hashtags_rank.filter(hashtags_rank.rank<=30) \
                                    .withColumn("res",concat_ws(':',col('words'),col('count')))\
                                    .groupBy('window').agg(collect_list('res'))
        hashtags_top.show(50,truncate=False)