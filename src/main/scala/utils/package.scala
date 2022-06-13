import scala.math.{pow, sqrt}

package object utils {
  def findDistance(point: Point, coordinate: Array[Double]): Double =
    sqrt(
      (point.coordinates zip coordinate).map {
        case (x, y) => pow(y - x, 2)
      }.sum
    )
}
