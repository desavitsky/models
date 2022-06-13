package optics

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import utils.Distances


class Optics(
    minPts: Int,
    radius: Double,
    distanceType: String = "EuclideanDistance"
) extends Serializable
    with Logging {

  @volatile private var pointsData = Array.empty[Point]

  /** it used to calculate the distance between processing point and all input
    * points
    *
    * @param points
    *   : original all points
    * @param coordinateValues
    *   : processing point
    * @param distanceType
    *   : defined in object Distance, like "EuclideanDistance",
    *   "LenvenshteinDistance"...
    * @tparam T
    *   : according to the type of the input points, like String,
    *   Array[Double]...
    * @return
    *   : distance between processing point and all input points
    */
  private def getDistance(
      points: Array[Array[Double]],
      coordinateValues: Array[Double],
      distanceType: String
  ): Array[Double] = {
    Distances.get(points, coordinateValues, distanceType)
  }

  /** update output sequence points
    *
    * @param points
    *   : the final output of optics.ParallelOptics
    * @param distance
    *   : distance between processing points and all points
    * @param id
    *   : the index of processing points in original data
    * @param opticsId
    *   : output order of points
    * @param hasNeighbor
    *   : is in neighbor sequence still has points
    * @return
    *   : undated points
    */
  private def update(
      points: Array[Point],
      distance: Array[Double],
      id: Long,
      opticsId: Long,
      hasNeighbor: Boolean = false
  ): (Array[Point], Boolean) = {
    val neiNum = distance.count(_ <= radius)
    var points_ = points
    val pointsT = points.zip(distance)
    if (neiNum > minPts) {
      val coreNei = distance.sorted.take(minPts + 1)
      val coreDis = coreNei(minPts)
      points_ = pointsT.map { p =>
        // Core point, set its output serial number as opticsId
        if (p._1.id == id) {
          p._1.coreDis = coreDis
          p._1.opticsId = opticsId
          p._1.processed = true
        }
        // Neighbor points, because one of they could be the next output, set there serial number as opticsId + 1,
        if (p._2 <= radius & p._1.id != id & !p._1.processed) {
          if (p._2 < coreDis)
            p._1.reachDis = Math.min(p._1.reachDis, coreDis)
          else
            p._1.reachDis = Math.min(p._1.reachDis, p._2)
          p._1.opticsId = opticsId + 1
        }
        // Updating the serial number of previous core points' neighbor
        if (p._2 > radius & p._1.opticsId == opticsId & !p._1.processed)
          p._1.opticsId = opticsId + 1
        p._1.getP
      }
    } else {
      points_ = pointsT.map { p =>
        // Mark p is not core point
        if (p._1.id == id) {
          p._1.notCore = true
          // if p is already a neighbor point, mark it as processed, and output it
          if (hasNeighbor) {
            p._1.opticsId = opticsId
            p._1.processed = true
          }
        }
        // Updating the serial number of previous core points' neighbor
        if (
          hasNeighbor & p._1.opticsId == opticsId & !p._1.processed & p._1.id != id
        )
          p._1.opticsId = opticsId + 1
        p._1.getP
      }
    }
    //    points_.checkpoint()
    (points_, neiNum > minPts | hasNeighbor)
  }

  def train(points: RDD[Array[Double]]): Array[Point] = {

    val pointsA = points
      .map(_.dropRight(1))
      .collect()

    assert(minPts >= 0, "minPts smaller than 0")

    val initStartTime = System.nanoTime()

    val pointsTemp = points.zipWithIndex().collect()
    // here can add a String information for each Point when call new Point(id, information)
    var points_ = pointsTemp.map { p =>
      Point(p._2, p._1.dropRight(1), if (p._1.last == 1.0) true else false)
    }
    val pointsT = pointsTemp.map { case (a: Array[Double], b: Long) =>
      (a.dropRight(1), b)
    }
    var opticsId: Long = 0

    while (points_.exists(p => !p.processed & !p.notCore)) {

      var hasOut = true
      var point = points_.filter(p => !p.processed & !p.notCore).head
      var distance = getDistance(
        points.collect(),
        pointsT.filter { p => p._2 == point.id }.head._1,
        distanceType
      )
      val temp = update(points_, distance, point.id, opticsId)
      points_ = temp._1
      hasOut = temp._2
      if (hasOut)
        opticsId += 1

      while (points_.exists(p => p.opticsId == opticsId)) {

        val hasNeighbor = true
        point = points_
          .filter(p => !p.processed & p.opticsId == opticsId)
          .minBy(p => p.reachDis)
        distance = getDistance(
          pointsA,
          pointsT.filter { p => p._2 == point.id }.head._1,
          distanceType
        )
        points_ = update(points_, distance, point.id, opticsId, hasNeighbor)._1
        opticsId += 1
      }
    }

    points_ = points_.sortBy(p => p.opticsId)

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(
      s"optics.ParallelOptics run took " + "%.3f".format(
        initTimeInSeconds
      ) + " seconds."
    )

    pointsData = points_
    points_
  }

  /** @param points
    *   : input data, org.apache.spark.rdd.RDD[T]
    * @tparam T
    *   : in this version, T is Array[Double] or String with URL format like
    *   "/a/b/c"
    * @return
    *   : a sequence of class Point, it records the necessary information to get
    *   clusters
    */

  def run(points: RDD[Array[Double]]): Array[Point] = {

    val pointsA = points.collect()

    assert(minPts >= 0, "minPts smaller than 0")

    val initStartTime = System.nanoTime()

    val pointsT = points.zipWithIndex().collect()

    var points_ = pointsT.map { p => new Point(p._2, p._1) }
    var opticsId: Long = 0

    while (points_.exists(p => !p.processed & !p.notCore)) {

      println(
        s"NOT PROCESSED ${points_.count(p => !p.processed & !p.notCore)}"
      )

      var hasOut = true
      var point = points_.filter(p => !p.processed & !p.notCore).head
      var distance = getDistance(
        points.collect(),
        pointsT.filter { p => p._2 == point.id }.head._1,
        distanceType
      )
      val temp = update(points_, distance, point.id, opticsId)
      points_ = temp._1
      hasOut = temp._2
      if (hasOut)
        opticsId += 1

      while (points_.exists(p => p.opticsId == opticsId)) {

        val hasNeighbor = true
        point = points_
          .filter(p => !p.processed & p.opticsId == opticsId)
          .minBy(p => p.reachDis)
        distance = getDistance(
          pointsA,
          pointsT.filter { p => p._2 == point.id }.head._1,
          distanceType
        )
        points_ = update(points_, distance, point.id, opticsId, hasNeighbor)._1
        opticsId += 1
      }
    }

    points_ = points_.sortBy(p => p.opticsId)

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(
      s"optics.ParallelOptics run took " + "%.3f".format(
        initTimeInSeconds
      ) + " seconds."
    )

    points_
  }

  def update(points: Array[Point]): (Array[Point], Status) = {
    val pointsA = points.map(_.coordinates)

    assert(minPts >= 0, "minPts smaller than 0")

    val initStartTime = System.nanoTime()

    val pointsT = pointsA.zipWithIndex
    var points_ = pointsT.map { p => Point(p._2, p._1) }
    var opticsId: Long = 0

    while (points_.exists(p => !p.processed & !p.notCore)) {

      var hasOut = true
      var point = points_.filter(p => !p.processed & !p.notCore).head
      var distance = getDistance(
        pointsA,
        pointsT.filter { p => p._2 == point.id }.head._1,
        distanceType
      )
      val temp = update(points_, distance, point.id, opticsId)
      points_ = temp._1
      hasOut = temp._2
      if (hasOut)
        opticsId += 1

      while (points_.filter(p => p.opticsId == opticsId).size > 0) {

        val hasNeighbor = true
        point = points_
          .filter(p => !p.processed & p.opticsId == opticsId)
          .sortBy(p => p.reachDis)
          .head
        distance = getDistance(
          pointsA,
          pointsT.filter { p => p._2 == point.id }.head._1,
          distanceType
        )
        points_ = update(points_, distance, point.id, opticsId, hasNeighbor)._1
        opticsId += 1
      }
    }

    points_ = points_.sortBy(p => p.opticsId)

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(
      s"optics.ParallelOptics run took " + "%.3f".format(
        initTimeInSeconds
      ) + " seconds."
    )

    val newPoint = points_
      .find(_.id == points.length - 1)
      .get

    val status =
      if (newPoint.reachDis == Double.MaxValue) Anomaly
      else Normal

    val finalPoints = if (
      status == Normal && newPoint.notCore && (0 < newPoint.opticsId && newPoint.opticsId < points_.length - 1) && !points_(
        (newPoint.opticsId - 1).toInt
      ).notCore && !points_(
        (newPoint.opticsId + 1).toInt
      ).notCore
    ) points_.filter(_.id == newPoint.id)
    else
      points_.drop(1).dropRight(1).find { p =>
        val prev = points_((p.opticsId - 1).toInt)
        val next = points_((p.opticsId + 1).toInt)
        prev.notCore && next.notCore && calcDistance(
          prev.coordinates,
          next.coordinates
        ) <= radius
      } match {
        case Some(value) => points_.filter(_.id == value.id)
        case None        => points_.dropRight(1)
      }
    (finalPoints, status)
  }

  def update(point: Point): Status = {
    val points = pointsData :+ point.copy(id_ = pointsData.length)
    val pointsA = points.map(_.coordinates)

    assert(minPts >= 0, "minPts smaller than 0")

    val initStartTime = System.nanoTime()

    val pointsT = pointsA.zipWithIndex
    // here can add a String information for each Point when call new Point(id, information)
    var points_ = pointsT.map { p => Point(p._2, p._1) }
    var opticsId: Long = 0

    //    points.persist()
    //    pointsT.persist()
    //    points_.persist()

    while (points_.exists(p => !p.processed & !p.notCore)) {

      var hasOut = true
      var point = points_.filter(p => !p.processed & !p.notCore).head
      var distance = getDistance(
        pointsA,
        pointsT.filter { p => p._2 == point.id }.head._1,
        distanceType
      )
      val temp = update(points_, distance, point.id, opticsId)
      points_ = temp._1
      hasOut = temp._2
      if (hasOut)
        opticsId += 1

      while (points_.filter(p => p.opticsId == opticsId).size > 0) {

        val hasNeighbor = true
        point = points_
          .filter(p => !p.processed & p.opticsId == opticsId)
          .sortBy(p => p.reachDis)
          .head
        distance = getDistance(
          pointsA,
          pointsT.filter { p => p._2 == point.id }.head._1,
          distanceType
        )
        points_ = update(points_, distance, point.id, opticsId, hasNeighbor)._1
        opticsId += 1
      }
    }

    points_ = points_.sortBy(p => p.opticsId)

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(
      s"optics.ParallelOptics run took " + "%.3f".format(
        initTimeInSeconds
      ) + " seconds."
    )

    val newPoint = points_
      .find(_.id == points.length - 1)
      .get

    val status =
      if (newPoint.reachDis == Double.MaxValue) Anomaly
      else Normal

    pointsData = if (points_.length > 2000) {
      if (
        status == Normal && newPoint.notCore && (0 < newPoint.opticsId && newPoint.opticsId < points_.length - 1) && !points_(
          (newPoint.opticsId - 1).toInt
        ).notCore && !points_(
          (newPoint.opticsId + 1).toInt
        ).notCore
      ) points_.filterNot(_.id == newPoint.id)
      else
        points_.drop(1).dropRight(1).find { p =>
          if (p.opticsId == Long.MaxValue) false
          else {
            val prev = points_((p.opticsId - 1).toInt)
            val next = points_((p.opticsId + 1).toInt)
            p.opticsId != Long.MaxValue && points_(
              (p.opticsId - 1).toInt
            ).notCore && points_(
              (p.opticsId + 1).toInt
            ).notCore && calcDistance(
              prev.coordinates,
              next.coordinates
            ) <= radius
          }
        } match {
          case Some(value) => points_.filterNot(_.id == value.id)
          case None        => points_.dropRight(1)
        }
    } else points_

    status

  }

  private def calcDistance(a: Array[Double], b: Array[Double]) =
    math.sqrt((a zip b).map { case (p1, p2) => p1 * p1 - p2 * p2 }.sum)

}

case class Point(
    id_ : Long,
    coordinates: Array[Double],
    anomaly: Boolean = false
) extends Serializable {
  val id = id_
  var reachDis = Double.MaxValue
  var coreDis = Double.MaxValue
  var processed = false
  var opticsId = Long.MaxValue
  var notCore = false

  def getP: Point = this
}
