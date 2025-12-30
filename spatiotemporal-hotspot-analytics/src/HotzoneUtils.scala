package spatial.analytics

object HotzoneUtils {

  /**
   * Checks whether a point lies inside (or on the boundary of) a rectangle.
   *
   * @param queryRectangle Rectangle string formatted as "x1,y1,x2,y2"
   * @param pointString    Point string formatted as "x,y"
   * @return true if the point is inside the rectangle, false otherwise
   */
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {

    if (queryRectangle == null || queryRectangle.isEmpty ||
        pointString == null || pointString.isEmpty) {
      false
    } else {
      try {
        val rect = queryRectangle.split(",")
        val point = pointString.split(",")

        if (rect.length != 4 || point.length != 2) {
          false
        } else {
          val x1 = rect(0).trim.toDouble
          val y1 = rect(1).trim.toDouble
          val x2 = rect(2).trim.toDouble
          val y2 = rect(3).trim.toDouble

          val px = point(0).trim.toDouble
          val py = point(1).trim.toDouble

          val minX = math.min(x1, x2)
          val maxX = math.max(x1, x2)
          val minY = math.min(y1, y2)
          val maxY = math.max(y1, y2)

          px >= minX && px <= maxX && py >= minY && py <= maxY
        }
      } catch {
        case _: NumberFormatException => false
      }
    }
  }
}
