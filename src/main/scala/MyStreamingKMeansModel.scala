import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

case class MyStreamingKMeans(decayFactor: Double) {

  private var model: MyStreamingKMeansModel = MyStreamingKMeansModel.empty

  def trainAndPredict(data: RDD[Vector]): Unit = this.synchronized {
    model = model.updateModel(data, decayFactor)
  }

  def trainAndPredict(data: Vector): Unit = this.synchronized {
    model = model.updateModel(data, decayFactor)
  }

  def setModel(model: MyStreamingKMeansModel): Unit =
    this.model = model
}

case class MyStreamingKMeansModel(
    override val clusterCenters: Array[Vector],
    clustersInfo: List[ClusterInfo]
) extends KMeansModel(clusterCenters)
    with Logging {

  def updateModel(
      data: RDD[Vector],
      decayFactor: Double
  ): MyStreamingKMeansModel = {
    val closest =
      data.map(point => (this.predict(point), (point, 1L))).collect().head._1

    logWarning(s"New point belongs to cluster $closest")

    val newClusterCenter: Vector = Vectors.dense(
      clusterCenters(closest).toArray.zip(data.collect().head.toArray).map {
        case (a, b) =>
          (a * clustersInfo(closest).nPoints * decayFactor + b) / (clustersInfo(
            closest
          ).nPoints + 1)
      }
    )

    val newClusterCenters =
      clusterCenters.patch(closest, Array(newClusterCenter), 1)

    val newClustersInfo = clustersInfo.map {
      case k if k.clusterIndex == closest => k.copy(nPoints = k.nPoints + 1)
      case k                              => k
    }

    MyStreamingKMeansModel(newClusterCenters, newClustersInfo)
  }

  def updateModel(
      point: Vector,
      decayFactor: Double
  ): MyStreamingKMeansModel = {
    val closest = predict(point)

    logWarning(s"New point belongs to cluster $closest")

//    val newClusterCenters: Array[Vector] = clusterCenters(closest).toArray.zip(point.toArray).map {
//      case (a, b) => (a * clustersInfo(closest).nPoints * decayFactor + b) / (clustersInfo(closest).nPoints + 1)
//    }.map(Vectors.dense(_))

    val newClusterCenter: Vector = Vectors.dense(
      clusterCenters(closest).toArray.zip(point.toArray).map { case (a, b) =>
        (a * clustersInfo(closest).nPoints * decayFactor + b) / (clustersInfo(
          closest
        ).nPoints + 1)
      }
    )

    val newClusterCenters: Array[Vector] =
      clusterCenters.patch(closest, Array(newClusterCenter), 1)

    val newClustersInfo = clustersInfo.map {
      case k if k.clusterIndex == closest => k.copy(nPoints = k.nPoints + 1)
      case k                              => k
    }

    logWarning(s"clustersInfo: ${newClustersInfo}")

    MyStreamingKMeansModel(newClusterCenters, newClustersInfo)
  }

}

object MyStreamingKMeansModel {
  def load(
      sparkContext: SparkContext
  )(model: KMeansModel): MyStreamingKMeansModel = {
    val data = sparkContext.textFile(
      "/Users/denis_savitsky/Downloads/spark-hes2/p/SCALED_TRAIN_NEW3_ONM.csv"
    )
    val parsedData: RDD[Vector] =
      data.map(s => Vectors.dense(s.split(",").map(_.toDouble))).cache()
    val clustersInfo = parsedData
      .map(model.predict)
      .collect()
      .groupBy(identity)
      .map { case (i, n) =>
        ClusterInfo(i, n.length)
      }
      .toList

    MyStreamingKMeansModel(model.clusterCenters, clustersInfo)
  }

  val empty = MyStreamingKMeansModel(Array.empty, List.empty)
}

private[temp] case class ClusterInfo(clusterIndex: Int, nPoints: Int)
