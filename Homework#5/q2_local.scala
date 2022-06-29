import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

val conf = new SparkConf()
val sc=  new SparkContext()

val graph = GraphLoader.edgeListFile(sc,"/home/ben/HW#5/edge_list.txt")
val dag = GraphLoader.edgeListFile(sc,"/home/ben/HW#5/dag_edge_list.txt") 


/*
    Q2(a): Graph Properties/Attributes
*/

// All the Edges/Triplets
// graph.edges.collect.foreach(println(_))
// Number of Edges
println("\n Number of Edges: " + graph.numEdges)
// Number of Vertices
println("\n Number of Vertices: " + graph.numVertices)
// Vertex with the largest in-degree/out-degree:
// https://spark.apache.org/docs/latest/graphx-programming-guide.html#computing-degree-information
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}
val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)

/**
 *    Q2(b): ConnectedComponents
 *    Reference: https://blog.csdn.net/ZH519080/article/details/104783312
 */
// Number of ConnectedComponents
graph.connectedComponents()
     .vertices
     .map(_.swap)
     .groupByKey()
     .count()
// Number of vertices exist in the largest connected components
graph.connectedComponents()
     .vertices
     .map(_.swap)
     .groupByKey()
     .map(x=>(x._1, x._2.size))
     .collect()

// Number of stronglyConnectedComponents
graph.stronglyConnectedComponents(3)
     .vertices
     .map(_.swap)
     .groupByKey()
     .count()

graph.stronglyConnectedComponents(3)
     .vertices
     .map(_.swap)
     .groupByKey()
     .map(x=>(x._1, x._2.size))
     .collect()

/**
 *   Q2(c): Personalized PageRank
 *   Reference: https://blog.csdn.net/lsshlsw/article/details/41176093
 */
// Top-20 PR Scores with respect to vertex 4330 and 5730
graph.personalizedPageRank(4330,tol=0.01,resetProb=0.15)
     .vertices.top(20)(Ordering.by(_._2))
     //.sortBy(_._2,ascending=false).take(20)
graph.personalizedPageRank(5730,tol=0.01,resetProb=0.15)
     .vertices.top(20)(Ordering.by(_._2))
// Top-2000 PR scores nodes subgraph():167 edges
val vertices_5730 = graph.personalizedPageRank(5730,tol=0.01,resetProb=0.15)
     .vertices.top(2000)(Ordering.by(_._2)).map(_._1)
graph.subgraph(vpred = (vid,v) => vertices_5730.contains(vid), epred=edge=>(vertices_5730.contains(edge.srcId) && vertices_5730.contains(edge.dstId))).edges.count()


/**
 *   Q2(d): LabelPorpagation.run() ====> detect # of communities & # of vertices in the largest community.
 */
val comm_count = lib.LabelPropagation.run(graph,maxSteps=50).vertices.map(_._2).distinct().count()
val top_comm_vertices_count = lib.LabelPropagation.run(graph,maxSteps=50).vertices.map(x=>(x._2,1)).reduceByKey(_+_).top(20)(Ordering.by(_._2))

/**
 *   Q2(e): Find the largest distance with Pregel
 *   Reference: http://t.csdn.cn/x4H9g
 */
def sendMsg(ec: EdgeContext[Int,Int,Int]): Unit = {
     ec.sendToDst(ec.srcAttr+1)
}
// the vertex will contain the highest value, or distance, over all the messages
def mergeMsg(a: Int, b: Int): Int = {
     math.max(a,b)
}
def propagateEdgeCount(g:Graph[Int,Int]):Graph[Int,Int] = {
     val verts = g.aggregateMessages[Int](sendMsg, mergeMsg)
     val g2 = Graph(verts, g.edges)
     // in Tuple2[vertexId, Tuple2[old vertex data, new vertex data]]
     val check = g2.vertices.join(g.vertices).
                              map(x => x._2._1 - x._2._2).
                              reduce(_ + _)
     println(s"check: $check")
     if (check > 0) // If there are no new information
          propagateEdgeCount(g2)
     else
          g
}
val dag = GraphLoader.edgeListFile(spark.sparkContext, "data/dag_edge_list.csv")
val initialGraph = dag.mapVertices((_,_) => 0)
propagateEdgeCount(initialGraph).vertices.collect

val truth = propagateEdgeCount(initialGraph)

val g_res = initialGraph.pregel(0,Int.MaxValue,EdgeDirection.Out)(
    (vid,vd,rcvMsg) => rcvMsg, // Update the graph
    triplet => Iterator((triplet.dstId, triplet.srcAttr + 1)), // Map the triplets, and send message to the destination of each triplet
    (a,b) => math.max(a,b) // Reduce all the received messages
)
val res = g_res.vertices.top(20)(Ordering.by(_._2))
truth.vertices.top(20)(Ordering.by(_._2))
