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