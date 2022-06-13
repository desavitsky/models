package kmeans

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MainElbow extends App {
  val sparkSession = SparkSession.builder
    .master("local[6]")
    .appName("spark-app")
    .getOrCreate()

  val sparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("ERROR")
  println("*** Initialized Spark instance.")
  val streamingContext =
    new StreamingContext(sparkContext, batchDuration = Seconds(20))

  // Load and parse the data
  val data = sparkContext.textFile(
    "/Users/denis_savitsky/Downloads/train0.csv"
  )
  val parsedData =
    data
      .map(s => Vectors.dense(s.split(",").map(_.toDouble).dropRight(1)))
      .cache()

  (1 to 10).foreach { numClusters =>
    val numIterations = 10000

    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(
      s"Within Set Sum of Squared Errors = $WSSSE (number of clusters: $numClusters)"
    )
//    clusters.save(
//      sparkContext,
//      s"/home/spark/IdeaProjects/spark-hes2/p/KMeansModel${numClusters}"
//    )
  }
}
