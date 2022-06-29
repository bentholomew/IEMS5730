import org.apache.spark.{SparkConf,SparkContext}
 
object wordCount{
    def main(args:Array[String]){
        val conf = new SparkConf().setAppName("wordCount")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("file:///opt/spark/data/shakespeare/*")
        val words = lines.flatMap(_.split(" ")).filter(word => word != "")
        val pairs = words.map(word => (word,1))
        val wordscount = pairs.reduceByKey(_ + _ )
        wordscount.collect.foreach(println)
        sc.stop()
    }
}