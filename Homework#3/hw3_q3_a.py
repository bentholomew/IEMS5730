from pyspark.sql import SparkSession
#from pyspark import SparkContext, SparkConf
#from pyspark.sql import SQLContext,Row
#conf = SparkConf().setAppName("hw3_q3")
#sc = SparkContext(conf=conf)
#sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("hw3_q3").getOrCreate()
crime = spark.read.csv("Crime_Incidents_in_2013.csv",header=True)
crime.createOrReplaceTempView("crime")
crime_a = spark.sql("SELECT CCN,REPORT_DAT,OFFENSE,METHOD,END_DATE,DISTRICT FROM crime")
crime_a.show()
crime_a.count()
crime_a.na.drop().count()
