import org.graphframes.GraphFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SocialNetworks {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Arg 1 : Input file location ; Arg 2 : Output directory")
    }

    // create Spark context wih Spark configuration

    val sparkConf = new SparkConf().setAppName("Social Network Analysis using Graphx")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SparkSession.Builder()
      .config(sparkConf)
      .getOrCreate()

    import sqlContext.implicits._

    //Read input file data from the arguments
    val inputDataPath = args(0)

    //Set the output directory from the arguments
    val outputPath = args(1)

    //read the input file
    val inputData = sqlContext.read.option("header", "true").option("delimiter", "\t").csv(inputDataPath).toDF("src", "dst")

    //Selecting source and destination vertex from input data
    val srcVertex = inputData.select("src").distinct()
    val dstVertex = inputData.select("dst").distinct()

    //Calculating all unique vertex
    val graphVertices = dstVertex.union(srcVertex).toDF("id").distinct()

    // Selecting the edges from the data
    val graphEdges = inputData.select("src", "dst")

    // Creating graph using GraphFrames from Vertices and Edges
    val finalGraph = GraphFrame(graphVertices, graphEdges)

    // Finding out degree from the graph
    val outDegree = finalGraph.outDegrees

    // Finding in degree from the graph
    val inDegree = finalGraph.inDegrees

    // Calculating page rank for the graph
    val pageRank = finalGraph.pageRank.resetProbability(0.15).maxIter(10).run()

    // Calculating connected components for the graph
    sparkContext.setCheckpointDir("/checkpoints")
    val cc = GraphFrame(graphVertices, graphEdges.sample(false, 0.1)).connectedComponents.run()

    // Calculating Triangle count for the graph
    val triangleCount = finalGraph.triangleCount.run()

    // a. Find the top 5 nodes with the highest outdegree and find the count of the number of outgoing edges in each
    outDegree.orderBy(desc("outDegree")).limit(5)
      .coalesce(1).
      write.mode(SaveMode.Append).
      format("csv").
      option("header", "true").
      save(outputPath)

    //b. Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges in each
    inDegree.orderBy(desc("inDegree")).limit(5)
      .coalesce(1).
      write.mode(SaveMode.Append).
      format("csv").
      option("header", "true").
      save(outputPath)

    //c. Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank values.
    pageRank.vertices.orderBy(desc("pagerank")).select("id", "pagerank").limit(5)
      .coalesce(1).
      write.mode(SaveMode.Append).
      format("csv").
      option("header", "true").
      save(outputPath)

    //d. Run the connected components algorithm on it and find the top 5 components with the largest number of nodes.
    cc.where("component != 0").orderBy(desc("component")).limit(5)
      .coalesce(1).
      write.mode(SaveMode.Append).
      format("csv").
      option("header", "true").
      save(outputPath)

    //e. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the largest triangle count. In case of ties, you can randomly select the top 5 vertices.
    triangleCount.select("id", "count").orderBy(desc("count")).limit(5)
      .coalesce(1).
      write.mode(SaveMode.Append).
      format("csv").
      option("header", "true").
      save(outputPath)

  }
}
