import org.apache.spark.mllib.linalg.Vector
import utils.findDistance

case class KeepAllStreamingModel private (
    @volatile var N: Int,
    @volatile var mean: Point,
    @volatile var meanDistance: Double,
    @volatile var sqrSum: Double,
    @volatile var sum: Double
) {
  def update(point: Vector): Status = {
    val coordinate = point.toArray
    val std = math.sqrt(
      (sqrSum - math.pow(sum, 2) / N) / N
    )
    val distance = findDistance(mean, coordinate)
    if (distance <= meanDistance + 2 * std) {
      sqrSum += math.pow(distance, 2)
      sum += distance
      mean = Point(
        (mean.coordinates.map(_ * N) zip coordinate)
          .map { case (x, y) => x + y }
          .map(_ / (N + 1))
      )
      meanDistance = (meanDistance * N + distance) / (N + 1)
      N += 1
      Normal
    } else Anomaly
  }
}

object KeepAllStreamingModel {
  def make(points: List[Point]): KeepAllStreamingModel = {
    val mean = Point(
      points.map(_.coordinates).transpose.map(f => f.sum / f.size)
    )
    val distances = points.map(findDistance(_, mean.coordinates.toArray))
    val meanDistance = distances.sum / distances.size
    KeepAllStreamingModel(
      points.size,
      mean,
      meanDistance,
      points
        .map(p => math.pow(findDistance(p, mean.coordinates.toArray), 2))
        .sum,
      points.map(p => findDistance(p, mean.coordinates.toArray)).sum
    )
  }
}
