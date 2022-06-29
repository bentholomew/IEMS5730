from pyspark import SparkContext, SparkConf

def rank_count(x):
    res = [] 
    size = len(x[1][0])
    for link in x[1][0]:
        res.append((link,x[1][1]/size))
    return res

if __name__=="__main__":
    num_partitions = 6 #num_partitions = 8/10/16
    conf = SparkConf().setAppName("hw3_q2_b_{}".format(num_partitions))
    sc = SparkContext(conf=conf)
    lines = sc.textFile("hdfs://master:54310/user/hduser/web-Google").map(lambda x:x.split("\t"))
    links = lines.groupByKey().mapValues(tuple).partitionBy(num_partitions).persist()
    ranks = links.mapValues(lambda x:1)
    # PageRank calcualation for 10 iterations
    for i in range(10):
        contribs = links.join(ranks).flatMap(rank_count)
        ranks = contribs.reduceByKey(lambda x,y:x+y).mapValues(lambda x:0.15+0.85*x)
    # Sort & Save
    ranks.sortBy(lambda x:x[1],ascending=False).saveAsTextFile("hw3_q2_b_{}".format(num_partitions))