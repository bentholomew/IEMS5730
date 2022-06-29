<h1 align=center> IEMS5730 Homework#5 </h1>
<p align=center> 1155162635 LIU Zijian</p>

<h2 align=center>Declaration</h2>

<img src="Screenshots\Declaration.jpg" style="zoom:50%;" />



[TOC]
## **Q1 [35 Marks]** GraphFrames[^1][^2]

Write the python scripts in `q1.py` 

```python
from pyspark.sql import SQLContext,SparkSession
from graphframes import *

if __name__=="__main__":

    ''' Prepare the context and build the graph '''
    spark = SparkSession.builder.appName("hw5_q1").getOrCreate()
    e = spark.read.csv(r"mooc_actions.tsv",sep='\t',header=True)\
                .withColumnRenamed("USERID","src")\
                .withColumnRenamed("TARGETID","dst")\
                .withColumnRenamed("TIMESTAMP","timestamp")
    v = spark.read.csv(r"vertices.tsv",sep='\t',header=True)
    g = GraphFrame(v, e)

    print("\n***********  Q1(a): Report Graph Properties  *********** ")
    print("The Number of vertices: {}".format(g.vertices.count()))
    print("The Number of users: {}".format(g.filterVertices("type=='User'").vertices.count()))
    print("The Number of Course Activities: {}".format( g.filterVertices("type=='Course Activity'").vertices.count()))
    print("The Number of edges: {}".format(g.edges.count()))
    print("The vertex with the largest in-degree: {}".format(g.inDegrees.rdd.sortBy(lambda x:x[1],ascending=False).take(1)))
    print("The vertex with the largest out-degree: {}".format(g.outDegrees.rdd.sortBy(lambda x:x[1],ascending=False).take(1)))

    
    print("\n***********  Q1(b): Filter & Create Subgraphs  *********** ")
    sg = g.filterEdges("timestamp > 10000 AND timestamp<=50000").dropIsolatedVertices()
    print("The Number of vertices: {}".format(sg.vertices.count()))
    print("The Number of edges: {}".format(sg.edges.count()))

    
    print("\n***********  Q1(c): Motifs/Patterns Search  *********** ")
    motif1 = sg.find("(v1)-[e1]->(v3);(v2)-[e2]->(v3)").filter("v1.id!=v2.id").filter("e1.timestamp<e2.timestamp")
    motif2 = sg.find("(v1)-[e1]->(v2);(v2)-[e2]->(v3)")
    motif3 = sg.find("(v1)-[e1]->(v2);(v4)-[e4]->(v2);(v1)-[e3]->(v3);(v4)-[e2]->(v3)")\
               .filter("v1.id!=v4.id AND v2.id!=v3.id")\
               .filter("e1.timestamp<e2.timestamp AND e2.timestamp<e3.timestamp AND e3.timestamp<e4.timestamp")
    motif4 = sg.find("(v2)-[e1]->(v1);(v2)-[e3]->(v3);(v4)-[e4]->(v3);(v4)-[e2]->(v5)")\
               .filter("v2.id!=v4.id AND v1.id!=v3.id AND v3.id!=v5.id AND v1.id!=v5.id")\
               .filter("e1.timestamp<e2.timestamp AND e2.timestamp<e3.timestamp AND e3.timestamp<e4.timestamp")
    motifs = zip([1,2,3,4],[motif1.count(),motif2.count(),motif3.count(),motif4.count()])
    for motif in motifs:
        print("The Number of Occurences of motif#{}: {}".format(motif[0],motif[1]))
```

Upload the files to HDFS, and submit the code to IE DIC:

```shell
$ hadoop dfs -copyFromLocal mooc_actions.tsv vertices.tsv ./
$ spark-submit --repositories https://repos.spark-packages.org \
               --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11 \
               --master yarn \
               --deploy-mode cluster \
               --files mooc_actions.tsv,vertices.tsv \
               q1.py
```

Check result from the `stdout` :

![](Screenshots\Q1-res.jpg)



## **Q2 [35 Marks]** GraphX[^3][^4]
Write the python scripts in `q2.scala`

```scala
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


object Q2App {
     
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
        if (a._2 > b._2) a else b
    }

    def main(args: Array[String]){
        val sc = new SparkContext()
        val graph = GraphLoader.edgeListFile(sc,"edge_list.txt")

        /**
        *     Q2(a): Graph Properties/Attributes
        **/
        println("********** Q2(a): Graph Properties/Attributes **********")
        println("Number of Edges: " + graph.numEdges)
        println("Number of Vertices: " + graph.numVertices)
        val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
        val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
        println(s"The Vertex with the largest in-degree: $maxInDegree")
        println(s"The Vertex with the largest out-degree: $maxOutDegree")

        /**
        *    Q2(b): ConnectedComponents
        *    Reference: https://blog.csdn.net/ZH519080/article/details/104783312
        **/
        println("********** Q2(b): Connected Components/Strongly Connected Components **********")
        val numConnectedComponents = graph.connectedComponents().vertices
                                                                .groupBy(_._2)
                                                                .count()
        val numVerticesInLargest = graph.connectedComponents().vertices
                                                              .groupBy(_._2)
                                                              .map(x=>(x._1, x._2.size))
                                                              .top(1)(Ordering.by(_._2)).mkString("\n")
        // max-iter=1, if > 1, raises OOM from IEDIC, 
        // cannot be solved by configuring --num-executors or executor-memory
        // solved by configuring --driver-memory 15G
        val numStronglyConnectedComponents = graph.stronglyConnectedComponents(3).vertices.map(_._2).distinct.count()  
        println(s"Number of Connected Components: $numConnectedComponents")
        println(s"Number of Vertices in the largest Connected Component: $numVerticesInLargest")
        println(s"Number of Strongly Connected Components: $numStronglyConnectedComponents")


        /**
        *   Q2(c): Personalized PageRank
        *   Reference: https://blog.csdn.net/lsshlsw/article/details/41176093
        * 			   https://blog.csdn.net/hanweileilei/article/details/89764466
        **/
        println("********** Q2(c): Personalized PageRank **********")
        val top20PPR4330 = graph.personalizedPageRank(4330,tol=0.01,resetProb=0.15)
                                        .vertices
                                        .top(25)(Ordering.by(_._2)).mkString("\n") // Include the node itself, with 5 more nodes
                                    
        val top20PPR5730 = graph.personalizedPageRank(5730,tol=0.01,resetProb=0.15)
                                        .vertices
                                        .top(25)(Ordering.by(_._2)).mkString("\n") // Include the node itself, with 5 more nodes

        val vertices5730 = graph.personalizedPageRank(5730,tol=0.01,resetProb=0.15)
                                .vertices.top(2000)(Ordering.by(_._2)).map(_._1)
        val numSubgraphsEdgesTop2000 = graph.subgraph(
            vpred = (vid,v) => vertices5730.contains(vid) 
            //epred = edge=> (vertices5730.contains(edge.srcId) && vertices5730.contains(edge.dstId)) //No need
            ).edges.count()

        println(s"Top 20 PPR Nodes to 4330: $top20PPR4330")
        println(s"Top 20 PPR Nodes to 5730: $top20PPR5730")
        println(s"Number of Edges in the subgraph of Top2000 PPR Nodes: $numSubgraphsEdgesTop2000")

        
        /**
        *   Q2(d): LabelPropagation.run() ====> detect # of communities & # of vertices in the largest community.
        **/
        println("********** Q2(d): Community Detection with LabelPropagation **********")
        val numCommunities = lib.LabelPropagation.run(graph,maxSteps=50)
                                            .vertices.map(_._2).distinct().count()
        val topCommVerticesCount = lib.LabelPropagation.run(graph,maxSteps=50)
                                                        .vertices.map(x=>(x._2,1))
                                                    	.reduceByKey(_+_).top(10)(Ordering.by(_._2)).mkString("\n")

        println(s"Number of Communities: $numCommunities")
        println(s"Number of Vertices in the Largest Communities (Top10): $topCommVerticesCount")


        /**
        *   Q2(e): Find the largest distance with Pregel
        *   Reference: http://t.csdn.cn/x4H9g
        **/
        println("********** Q2(e): Find the largest distance from root with Pregel **********")
        val dag = GraphLoader.edgeListFile(sc, "dag_edge_list.txt")
        val initialGraph = dag.mapVertices((_,_) => 0)
        val g_res = initialGraph.pregel(0, Int.MaxValue, EdgeDirection.Out)(
                                    (vid,vd,rcvMsg) => rcvMsg, // Update the graph vertices attr
                                    triplet => Iterator((triplet.dstId, triplet.srcAttr + 1)), // Map the triplets, and send message to the destination of each triplet
                                    (a,b) => math.max(a,b) // Reduce all the received messages
                                )
        val res = g_res.vertices.top(20)(Ordering.by(_._2)).mkString("\n")
        println(s"top 20 nodes with the largest distance from the root: $res")

        sc.stop()

    }
}

```

Package with `sbt`: `$ sbt package` (The src file must be under the location `./src/main/scala`)

```
name := "IESM5730 Homework5 Q2"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.0"
```

Get the jar, and submit the jar to IE DIC, with additional memory / #executors configuration

```shell
$ spark-submit --class "Q2App" \
               --master yarn \
               --deploy-mode cluster \
               --driver-memory 15G \
               --num-executors 20 \
               iesm5730-homework5-q2_2.11-1.0.jar
```

Check result from the `stdout` 

![](Screenshots\Q2-res.png)

\** Note that for `stronglyConnectedComponents` , the `maxIter` param is set to be 3 since a larger value will cause `JavaHeapSpaceError` , and this could not be solved by configuring the `--num-executors` or `--executor-memory` , and could be solved by configuring `--driver-memory` .  A further test with spark-shell on the IE-DIC indicates : `stronglyConnectedComponents(1):169343` `stronglyConnectedComponents(4):141597 ` and `stronglyConnectedComponents(5):141346`

![](Screenshots\Q2-stronglyConnectedComponents.jpg)





## **Q3 [30 Marks]** HBase

### **(a) [15 Marks]** Upload data into HBase with Bulk Loading[^9]

Prerequisite: Add a unique row key for each row in the googlebooks-b dataset with the following python scripts 

```python
'''Option#1: with pandas'''
# import pandas as pd
# gbook = pd.read_csv("googlebooks-eng-all-1gram-20120701-b",sep='\t',header=None)
# gbook.to_csv("gbook_b_with_rowkey",sep='\t',header=None)

'''Option2: FIle I/O'''
src = 'googlebooks-eng-all-1gram-20120701-b'
dst = 'gbook_b_with_rowkey'

with open(src,'r') as f1:
    with open(dst,"w") as f2:
        index = 0
        for line in f1.readlines():
            f2.write(str(index) + "\t" + line)
            index += 1
```

![](Screenshots\Q3-gen-rowkey.jpg)

\** `Rowkey` starts from **0** to **61551916**: for spliting among the datanodes, the rowkeys separators are designed to **[20, 40, 60]**

(1) Upload the file onto HDFS, and (2) create a table (3) convert to HFiles with `ImportTsv ` (4) Load the HFiles into the table 

```shell
# 1. Upload the file to HDFS
$ hadoop dfs -copyFromLocal gbook_b_with_rowkey 

# 2. Create a table(column-family:gbook) via Hbase Shell first, total 4 splits
$ hbase shell
> create 'hw5_q3_1155162635', 'gbook', SPLITS => ['20', '40', '60']

# 3. Generate HFiles with ImportTsv, columns = [bigram, year, match_count, volume_count]
$ hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
        -Dimporttsv.columns=HBASE_ROW_KEY,gbook:bigram,gbook:year,gbook:match_count,gbook:volume_count \
        -Dimporttsv.bulk.output=/user/s1155162635/hbase/hw5_q3 \
        hw5_q3_1155162635 \
        /user/s1155162635/gbook_b_with_rowkey

# 4. Load the HFiles into the created table
$ hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /user/s1155162635/hbase/hw5_q3 hw5_q3_1155162635

# 5. Check the result
$ hbase shell
> describe 'hw5_q3_1155162635'
> scan 'hw5_q3_1155162635', {FILTER => "PageFilter(1)"}

#X: Local Test
#$ bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
#    -Dimporttsv.columns=HBASE_ROW_KEY,gbook:bigram,gbook:year,gbook:match_count,gbook:volume_count \
#    hw5_q3_1155162635 \
#    file:///home/ben/HW#5/gbook_b_with_rowkey
```

Result

![](Screenshots\Q3-a.jpg)



### **(b) [15 Marks]** Insert, filter, and delete records with hbase-shell[^8]

Insert the record, with rowkey as 0000

```shell
> put 'hw5_q3_1155162635', '0000', 'gbook:bigram', 'ierg4330'
> put 'hw5_q3_1155162635', '0000', 'gbook:year', '2019'
> put 'hw5_q3_1155162635', '0000', 'gbook:match_count', '100'
> put 'hw5_q3_1155162635', '0000', 'gbook:volume_count', '4'
> get 'hw5_q3_1155162635', '0000'
```

![](Screenshots\Q3-b-1.jpg)

Get all the records in the Year 1671 with occurrences larger than 100. The former condition is mateched with `BinaryComparator`, and the latter condition is matched with `RegexStringComparator`

```shell
> scan 'hw5_q3_1155162635', {FILTER=>"SingleColumnValueFilter('gbook','year',=,'binary:1671') AND SingleColumnValueFilter('gbook','match_count',=,'regexstring:^[1-9][0-9]{2,}')"} 
```

![](Screenshots\Q3-b-2.jpg)

Delete the 14 records above one by one in the shell, this could be efficiently done with batch processing using HBase API in Java MapReduce Programs

```shell
> deleteall 'hw5_q3_1155162635','1839084'
> deleteall 'hw5_q3_1155162635','2401773'
> deleteall 'hw5_q3_1155162635','6547060'
> deleteall 'hw5_q3_1155162635','9493043'
> deleteall 'hw5_q3_1155162635','20302313'
> deleteall 'hw5_q3_1155162635','23178287'
> deleteall 'hw5_q3_1155162635','29638626'
> deleteall 'hw5_q3_1155162635','34905819'
> deleteall 'hw5_q3_1155162635','36856772'
> deleteall 'hw5_q3_1155162635','39944269'
> deleteall 'hw5_q3_1155162635','42304111'
> deleteall 'hw5_q3_1155162635','49648662'
> deleteall 'hw5_q3_1155162635','49801949'
> deleteall 'hw5_q3_1155162635','61040552'
# Validation: Re-query the above conditions 
> scan 'hw5_q3_1155162635', {FILTER=>"SingleColumnValueFilter('gbook','year',=,'binary:1671') AND SingleColumnValueFilter('gbook','match_count',=,'regexstring:^[1-9][0-9]{2,}')"} 
```

![](Screenshots\Q3-b-3.jpg)



## **Q4 [20 Bonus]** SparkML

### **(a) [10 Marks]** Predict with ALS[^5]

Write the following python scripts to `q4_1.py`

```python
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating 
from pyspark import SparkConf, SparkContext


if __name__=="__main__":
    sc = SparkContext.getOrCreate()
    ratings = sc.textFile('ratings.csv').filter(lambda x:x[0]!='u')\
                                        .map(lambda line:line.split(",")[:-1])\
                                        .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

    rank = 50 # Dimensions/Clusters
    numIter = 10
    lambda_ = 0.01
    model = ALS.train(ratings,rank,numIter,lambda_)

    for user in [1,1001,10001]:
        print(model.recommendProducts(user,10)) # print the recommendation list

```

Upload data files to HDFS, and submit the code to IE DIC:

```shell
$ hadoop dfs -copyFromLocal ratings.csv 
$ spark-submit --repositories https://repos.spark-packages.org \
               --master yarn \
               --deploy-mode cluster \
               q4_1.py
```

Check result from the `stdout` <u>(Please zoom in on the PDF to check the high-resolution screenshot)</u>

![](Screenshots\Q4-1-res.png)



### **(b) [10 Marks]** Performance Evaluation[^6][^7]

Write the following python scripts to `q4_2.py` 

```Python
from pyspark.sql import SQLContext,SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.recommendation import ALS

if __name__=="__main__":
    spark = SparkSession.builder.appName("hw5_q4").config('spark.debug.maxToStringFields', '50').getOrCreate()
    ratings = spark.read.csv("ratings.csv", header=True, inferSchema=True)
    ratings = ratings[['userId','movieId','rating']]
	
    
    '''************* Original Model & paramGrid: Raise py4j connection error *************'''
    # Estimator/Model
    als = ALS(userCol='userId',
              itemCol='movieId',
              ratingCol='rating',
              coldStartStrategy='drop')
    
    # ParamGrid
    param_iters = [5, 10, 20] # Too many will cause OOM
    param_ranks = [20, 50, 100]
    param_regs = [0.1 ,0.01, 0.001]
    paramGrid = ParamGridBuilder().addGrid(als.maxIter,param_iters)\
                                  .addGrid(als.rank,param_ranks)\
                                  .addGrid(als.regParam,param_regs)\
                                  .build()

    '''************* Alternative: Only GridSearch 2 Params, Fix Iteration at 10************* '''
    # Estimator/Model
    # als = ALS(maxiter=10,
    #           userCol='userId',
    #           itemCol='movieId',
    #           ratingCol='rating',
    #           coldStartStrategy='drop')
    # # ParamGrid        
    # param_ranks = [50, 150, 300]
    # param_regs = [0.1 , 0.001]
    # paramGrid = ParamGridBuilder().addGrid(als.rank,param_ranks)\
    #                               .addGrid(als.regParam,param_regs)\
    #                               .build()

    # Evaluation Metrics: MSE
    evaluator = RegressionEvaluator(metricName="mse", 
                                    labelCol="rating", 
                                    predictionCol="prediction") 

    # Build the crossvalidator/gridsearch
    cv = CrossValidator(estimator=als,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator,
                    parallelism=10,
                    numFolds=5)
    
    # Fit the data into the built Cross Validator
    model = cv.fit(ratings)
    
    # Best Model
    best_model = model.bestModel
    print("**Best Model**")
    print("  Rank:", best_model._java_obj.parent().getRank())
    print("  MaxIter:", best_model._java_obj.parent().getMaxIter())
    print("  RegParam:", best_model._java_obj.parent().getRegParam())

    # Performance Comparison
    print(list(zip(model.avgMetrics,paramGrid)))

```

Submit the code to IE DIC:

```shell
$ spark-submit --repositories https://repos.spark-packages.org \
               --master yarn \
               --deploy-mode cluster \
               --num-executors 20 \
               --executor-memory 20G \
               q4_2.py
```

Check result from the `stdout`

![](Screenshots\Q4-b.jpg)

\** The 4th line of the output is too long to display, manually copy as follows:

The best-performing model params are: `rank=20, maxIter=20, regParam=0.01` , the corresponding `MSE` is  `0.645`

The original model params are: `rank=50, maxiter=10, regParam=0.01`, the corresponding `MSE` is `0.735`

```
[
(0.6638157364674068, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 20, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.1, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 5}), 

(0.6504579857453394, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 20, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.1, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 10}), 

(0.644679205920437, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 20, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.1, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 20}), 

(0.6768781353780601, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 20, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.01, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 5}), 

(0.6744229334516993, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 20, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.01, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 10}), 

(0.6692345680799021, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 20, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.01, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 20}), 

(0.7534999664415627, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 20, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.001, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 5}), 

(0.7542434070026283, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 20, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.001, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 10}), 

(0.7837152860824489, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 20, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.001, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 20}), 

(0.6758575303977008, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 50, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.1, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 5}), 

(0.654239633098612, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 50, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.1, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 10}), 

(0.6451678764105399, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 50, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.1, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 20}), 

(0.7300047598617627, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 50, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.01, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 5}), 

(0.735006158974097, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 50, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.01, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 10}), 

(0.7282807734669117, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 50, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.01, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 20}), 

(0.9803521963342077, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 50, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.001, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 5}), 

(0.9768070103963835, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 50, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.001, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 10}), 

(1.026548379023117, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 50, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.001, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 20}), 

(0.6885638349204486, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 100, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.1, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 5}), 

(0.6570673816323003, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 100, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.1, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 10}), 

(0.6456112642537295, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 100, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.1, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 20}), 

(0.751215743623582, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 100, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.01, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 5}), 

(0.7570668830712921, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 100, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.01, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 10}), 

(0.753013199614891, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 100, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.01, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 20}), 

(1.25719202615276, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 100, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.001, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 5}), 

(1.1926798837122192, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 100, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.001, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 10}), 

(1.206599892067395, {Param(parent=u'ALS_4988b448c311552006b9', name='rank', doc='rank of the factorization'): 100, Param(parent=u'ALS_4988b448c311552006b9', name='regParam', doc='regularization parameter (>= 0).'): 0.001, Param(parent=u'ALS_4988b448c311552006b9', name='maxIter', doc='max number of iterations (>= 0).'): 20})

]
```



## **Reference**
[^1]: PySpark GraphFramePythonAPI: https://blog.csdn.net/weixin_38336546/article/details/113651068
[^2]: PySpark GraphFrames Basics: https://blog.csdn.net/weixin_39198406/article/details/104922642

[^3]:GraphX ConnectedComponents/StronglyConnectedComponents: https://blog.csdn.net/ZH519080/article/details/104783312
[^4]:GraphX Pregel Example: https://blog.csdn.net/hanweileilei/article/details/89764466
[^5]: Apache Spark MLlib for RDD: https://spark.apache.org/docs/latest/ml-guide.html
[^6]: Apache Spark MLlib Guide for Dataframe: https://spark.apache.org/docs/latest/ml-guide.html
[^7]: PySpark Collaborative Filtering with ALS: https://towardsdatascience.com/build-recommendation-system-with-pyspark-using-alternating-least-squares-als-matrix-factorisation-ebe1ad2e7679
[^8]:HBase Scan&Filter: https://blog.csdn.net/m0_37739193/article/details/73615016 / https://blog.csdn.net/SimpleSimpleSimples/article/details/105896682 
[^9]:HBase importTsv: https://blog.csdn.net/ZanShichun/article/details/68924309 / https://www.jianshu.com/p/2b4390310345



