package kmeans

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.concurrent.atomic.AtomicInteger

object KMeansInit extends App {
  val sparkSession = SparkSession.builder
    .master("local[*]")
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

  val numClusters = 5
  val numIterations = 100000

  val model = KMeans.train(parsedData, numClusters, numIterations)

  val labels =
    data
      .map(s => Vectors.dense(s.split(",").map(_.toDouble).takeRight(1)))
      .cache()
      .collect()

  val distrib = model.predict(parsedData).collect()

  val A1 = new AtomicInteger(0)
  val N1 = new AtomicInteger(0)
  val A2 = new AtomicInteger(0)
  val N2 = new AtomicInteger(0)
  val A3 = new AtomicInteger(0)
  val N3 = new AtomicInteger(0)
  val A4 = new AtomicInteger(0)
  val N4 = new AtomicInteger(0)
  val A0 = new AtomicInteger(0)
  val N0 = new AtomicInteger(0)

  val res = (labels zip distrib) foreach { case (v, n) =>
    if (v.toArray.head == 1.0) {
      if (n == 0) A0.incrementAndGet()
      else if (n == 1) A1.incrementAndGet()
      else if (n == 2) A2.incrementAndGet()
      else if (n == 3) A3.incrementAndGet()
      else A4.incrementAndGet()
    } else {
      if (n == 0) N0.incrementAndGet()
      else if (n == 1) N1.incrementAndGet()
      else if (n == 2) N2.incrementAndGet()
      else if (n == 3) N3.incrementAndGet()
      else N4.incrementAndGet()
    }
  }

  println(s"A1 = ${A1.get()}")
  println(s"N1 = ${N1.get()}")
  println(s"A2 = ${A2.get()}")
  println(s"N2 = ${N2.get()}")
  println(s"A3 = ${A3.get()}")
  println(s"N3 = ${N3.get()}")
  println(s"A4 = ${A4.get()}")
  println(s"N4 = ${N4.get()}")
  println(s"A0 = ${A0.get()}")
  println(s"N0 = ${N0.get()}")

}
