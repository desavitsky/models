import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ElbowMethod extends App {


  val sparkSession = SparkSession
    .builder
    .master("local[6]")
    .appName("spark-app")
    .getOrCreate()

  val sparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("ERROR")
  println("*** Initialized Spark instance.")
  val streamingContext = new StreamingContext(sparkContext, batchDuration = Seconds(20))

  // Load and parse the data
  val data = sparkContext.textFile("data/mllib/kmeans_data.txt")
  val parsedData = data.map(s => Vectors.dense(s.split(",").map(_.toDouble))).cache()

  (1 to 10).foreach { numClusters =>
      val numIterations = 20

      val clusters = KMeans.train(parsedData, numClusters, numIterations)

      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = clusters.computeCost(parsedData)
      println(s"Within Set Sum of Squared Errors = $WSSSE")
      clusters.save(sparkContext, s"target/org/apache/spark/KMeansExample/KMeansModel${numClusters}")
  }
}
