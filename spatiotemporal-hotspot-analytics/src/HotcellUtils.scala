package spatial.analytics

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {

  // Grid cell size (degrees). Used for x/y discretization.
  val coordinateStep: Double = 0.01

  /**
   * Convert raw input into a discrete grid coordinate.
   *
   * coordinateOffset:
   *   0 -> X from "(x,y)"
   *   1 -> Y from "(x,y)"
   *   2 -> Z from timestamp (day of month)
   */
  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    if (inputString == null || inputString.trim.isEmpty) return 0

    coordinateOffset match {
      case 0 =>
        // X: floor(x / step)
        val x = inputString.split(",")(0).replace("(", "").trim.toDouble
        math.floor(x / coordinateStep).toInt

      case 1 =>
        // Y: floor(y / step)
        val y = inputString.split(",")(1).replace(")", "").trim.toDouble
        math.floor(y / coordinateStep).toInt

      case 2 =>
        // Z: day of month from pickup timestamp
        val ts = timestampParser(inputString)
        dayOfMonth(ts)

      case _ =>
        0
    }
  }

  /**
   * Parse a timestamp string formatted as "yyyy-MM-dd HH:mm:ss".
   * Using HH (24-hour) avoids AM/PM ambiguity.
   */
  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    new Timestamp(parsedDate.getTime)
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(Calendar.DAY_OF_MONTH)
  }
}
