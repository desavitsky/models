import scala.math._
import org.apache.spark.mllib.linalg.Vector

sealed trait Status

case object Anomaly extends Status

case object Normal extends Status

case class Point(coordinates: List[Double])

class KnnStreamingModel private (
    @volatile var anomalies: List[Point],
    @volatile var normals: List[Point],
    val limit: Int,
    val N: Int
) {
  def update(data: Vector): Status = {
    val coordinate: Array[Double] = data.toArray
    val status = defineStatus(coordinate)
    status match {
      case Anomaly =>
        if (anomalies.size < limit)
          anomalies = anomalies ++ List(Point(coordinate.toList))
        else anomalies = anomalies.tail ++ List(Point(coordinate.toList))
      case Normal =>
        if (normals.size < limit)
          normals = normals ++ List(Point(coordinate.toList))
        else normals = normals.tail ++ List(Point(coordinate.toList))
    }
    status
  }

  private def findDistance(point: Point, coordinate: Array[Double]): Double =
    sqrt(
      (point.coordinates zip coordinate).map { case (x, y) =>
        pow(y - x, 2)
      }.sum
    )

  private def findTopHalfN(
      points: List[Point],
      coordinate: Array[Double]
  ): List[Double] =
    points
      .map(findDistance(_, coordinate))
      .sorted
      .take(ceil(N.toDouble / 2).toInt)

  private def defineStatus(coordinate: Array[Double]): Status = {
    val dec = (findTopHalfN(anomalies, coordinate).map((_, Anomaly)) ++
      findTopHalfN(normals, coordinate).map((_, Normal)))
      .sortBy(_._1)
      .take(N)
      .count(_._2 == Normal)
    if (dec > ceil(N / 2)) Normal else Anomaly
  }
}

object KnnStreamingModel {
  def create(anomalies: List[Point], normals: List[Point], limit: Int, N: Int) =
    new KnnStreamingModel(anomalies, normals, limit, N)
}
