package spatial.analytics

import org.apache.spark.sql.SparkSession

/**
 * Distributed Spatial Queries (SparkSQL + Scala)
 *
 * Build:
 *   sbt clean assembly
 *
 * Run (example):
 *   spark-submit --master local[*] --class spatial.analytics.SpatialQuery target/scala-2.11/CSE511-assembly-0.1.0.jar <outputPath> \
 *     range_query <points.tsv> <x1,y1,x2,y2> \
 *     range_join_query <points.tsv> <rectangles.tsv> \
 *     distance_query <points.tsv> <xp,yp> <D> \
 *     distance_join_query <pointsA.tsv> <pointsB.tsv> <D>
 *
 * Notes:
 * - Input files are tab-delimited with a single geometry string column in _c0.
 * - Coordinates are treated as planar space (Euclidean distance).
 * - Output path is accepted for compatibility even if results are not persisted there by default.
 */
object SpatialQuery extends App {

  // ----------------------------
  // Parsing helpers
  // ----------------------------
  private def parsePoint(s: String): (Double, Double) = {
    val a = s.split(",")
    require(a.length >= 2, s"Invalid point string: '$s'")
    (a(0).trim.toDouble, a(1).trim.toDouble)
  }

  private def parseRect(s: String): (Double, Double, Double, Double) = {
    val a = s.split(",")
    require(a.length >= 4, s"Invalid rectangle string: '$s'")
    val x1 = a(0).trim.toDouble; val y1 = a(1).trim.toDouble
    val x2 = a(2).trim.toDouble; val y2 = a(3).trim.toDouble
    (math.min(x1, x2), math.min(y1, y2), math.max(x1, x2), math.max(y1, y2))
  }

  // ----------------------------
  // Spatial predicates
  // ----------------------------
  private def containsRectPoint(queryRectangle: String, pointString: String): Boolean = {
    val (minX, minY, maxX, maxY) = parseRect(queryRectangle)
    val (px, py) = parsePoint(pointString)
    px >= minX && px <= maxX && py >= minY && py <= maxY
  }

  private def withinDistance(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val (x1, y1) = parsePoint(pointString1)
    val (x2, y2) = parsePoint(pointString2)
    math.hypot(x1 - x2, y1 - y2) <= distance
  }

  // ----------------------------
  // Data loading helpers
  // ----------------------------
  private def readTsvSingleCol(spark: SparkSession, path: String) =
    spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(path)

  // ----------------------------
  // Query runners
  // ----------------------------
  def runRangeQuery(spark: SparkSession, pointsPath: String, rect: String): Long = {
    val points = readTsvSingleCol(spark, pointsPath)
    points.createOrReplaceTempView("points")

    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) => containsRectPoint(queryRectangle, pointString)
    )

    spark.sql(s"SELECT * FROM points WHERE ST_Contains('$rect', points._c0)").count()
  }

  def runRangeJoinQuery(spark: SparkSession, pointsPath: String, rectsPath: String): Long = {
    val points = readTsvSingleCol(spark, pointsPath)
    points.createOrReplaceTempView("points")

    val rects = readTsvSingleCol(spark, rectsPath)
    rects.createOrReplaceTempView("rectangles")

    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) => containsRectPoint(queryRectangle, pointString)
    )

    spark.sql("SELECT * FROM rectangles, points WHERE ST_Contains(rectangles._c0, points._c0)").count()
  }

  def runDistanceQuery(spark: SparkSession, pointsPath: String, centerPoint: String, dist: String): Long = {
    val points = readTsvSingleCol(spark, pointsPath)
    points.createOrReplaceTempView("points")

    spark.udf.register(
      "ST_Within",
      (p1: String, p2: String, d: Double) => withinDistance(p1, p2, d)
    )

    val d = dist.toDouble
    spark.sql(s"SELECT * FROM points WHERE ST_Within(points._c0, '$centerPoint', $d)").count()
  }

  def runDistanceJoinQuery(spark: SparkSession, pointsAPath: String, pointsBPath: String, dist: String): Long = {
    val p1 = readTsvSingleCol(spark, pointsAPath)
    p1.createOrReplaceTempView("p1")

    val p2 = readTsvSingleCol(spark, pointsBPath)
    p2.createOrReplaceTempView("p2")

    spark.udf.register(
      "ST_Within",
      (a: String, b: String, d: Double) => withinDistance(a, b, d)
    )

    val d = dist.toDouble
    spark.sql(s"SELECT * FROM p1, p2 WHERE ST_Within(p1._c0, p2._c0, $d)").count()
  }

  // ----------------------------
  // Main
  // ----------------------------
  private val usage =
    """
      |Usage:
      |  <outputPath> [commands...]
      |
      |Commands (preferred snake_case; camelCase also accepted):
      |  range_query <points.tsv> <x1,y1,x2,y2>
      |  range_join_query <points.tsv> <rectangles.tsv>
      |  distance_query <points.tsv> <xp,yp> <D>
      |  distance_join_query <pointsA.tsv> <pointsB.tsv> <D>
      |""".stripMargin

  val spark = SparkSession.builder()
    .appName("distributed-spatial-analytics")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  if (args.isEmpty) {
    System.err.println(usage)
    spark.stop()
    System.exit(1)
  }

  // Kept for compatibility with prior run signature; not used by default.
  val _outputPath = args(0)

  private def normalizeCmd(s: String): String = s match {
    // accept older camelCase + newer snake_case
    case "rangeQuery"        => "range_query"
    case "rangeJoinQuery"    => "range_join_query"
    case "distanceQuery"     => "distance_query"
    case "distanceJoinQuery" => "distance_join_query"
    case other               => other
  }

  var i = 1
  var rq: Option[Long] = None
  var rjq: Option[Long] = None
  var dq: Option[Long] = None
  var djq: Option[Long] = None

  while (i < args.length) {
    normalizeCmd(args(i)) match {
      case "range_query" =>
        rq = Some(runRangeQuery(spark, args(i + 1), args(i + 2)))
        i += 3

      case "range_join_query" =>
        rjq = Some(runRangeJoinQuery(spark, args(i + 1), args(i + 2)))
        i += 3

      case "distance_query" =>
        dq = Some(runDistanceQuery(spark, args(i + 1), args(i + 2), args(i + 3)))
        i += 4

      case "distance_join_query" =>
        djq = Some(runDistanceJoinQuery(spark, args(i + 1), args(i + 2), args(i + 3)))
        i += 4

      case unknown =>
        System.err.println(s"Ignoring unknown token: $unknown")
        i += 1
    }
  }

  rq.foreach(c => println(s"Range Query Count: $c"))
  rjq.foreach(c => println(s"Range Join Query Count: $c"))
  dq.foreach(c => println(s"Distance Query Count: $c"))
  djq.foreach(c => println(s"Distance Join Query Count: $c"))

  spark.stop()
}
