package spatial.analytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object HotcellAnalysis {

  private def configureLogging(): Unit = {
    Seq("org.spark_project", "org.apache", "akka", "com").foreach { name =>
      Logger.getLogger(name).setLevel(Level.WARN)
    }
  }

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    configureLogging()
    import spark.implicits._

    // 1) Load taxi trip records (semicolon-delimited)
    val rawTrips = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "false")
      .load(pointPath)

    rawTrips.createOrReplaceTempView("nyctaxitrips")

    // 2) Compute integer cell coordinates (x, y, z)
    spark.udf.register("CalculateX", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 0))
    spark.udf.register("CalculateY", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 1))
    spark.udf.register("CalculateZ", (pickupTime: String)  => HotcellUtils.CalculateCoordinate(pickupTime, 2))

    val pickupCells = spark.sql(
      """
        |SELECT
        |  CalculateX(_c5) AS x,
        |  CalculateY(_c5) AS y,
        |  CalculateZ(_c1) AS z
        |FROM nyctaxitrips
      """.stripMargin
    )

    // 3) Study area bounds (converted to integer grid coordinates)
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY =  40.50 / HotcellUtils.coordinateStep
    val maxY =  40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31

    val minXi = math.floor(minX).toInt
    val maxXi = math.floor(maxX).toInt
    val minYi = math.floor(minY).toInt
    val maxYi = math.floor(maxY).toInt
    val minZi = minZ
    val maxZi = maxZ

    // Total number of cells in the study area (including empty cells)
    val numCells = (maxXi - minXi + 1).toLong *
      (maxYi - minYi + 1).toLong *
      (maxZi - minZi + 1).toLong
    val N = numCells.toDouble

    // 4) Count points per non-empty cell (restricted to study area)
    val cells = pickupCells
      .filter(col("x").between(minXi, maxXi) &&
        col("y").between(minYi, maxYi) &&
        col("z").between(minZi, maxZi))
      .groupBy("x", "y", "z")
      .agg(count(lit(1)).as("cnt"))
      .cache()

    // 5) Global statistics (mean & std) across ALL study cells
    // Empty cells contribute 0 count; handled via N in the formulas.
    val statsRow = cells
      .agg(
        sum(col("cnt").cast(DoubleType)).as("sumX"),
        sum(pow(col("cnt").cast(DoubleType), 2)).as("sumX2")
      )
      .first()

    val sumX  = Option(statsRow.getAs[java.lang.Double]("sumX")).map(_.doubleValue()).getOrElse(0.0)
    val sumX2 = Option(statsRow.getAs[java.lang.Double]("sumX2")).map(_.doubleValue()).getOrElse(0.0)

    val meanX = sumX / N
    val s = math.sqrt((sumX2 / N) - (meanX * meanX))

    // 6) Build 27-neighbor offsets
    val offsets = (-1 to 1).flatMap(dx => (-1 to 1).flatMap(dy => (-1 to 1).map(dz => (dx, dy, dz))))
    val offsetsDF = offsets.toDF("dx", "dy", "dz")

    // 7) Neighbor sum (sumW) and neighbor count (W) per cell
    val neighAgg = cells.as("c")
      .crossJoin(offsetsDF)
      .withColumn("nx", col("c.x") + col("dx"))
      .withColumn("ny", col("c.y") + col("dy"))
      .withColumn("nz", col("c.z") + col("dz"))
      .filter(col("nx").between(minXi, maxXi) &&
        col("ny").between(minYi, maxYi) &&
        col("nz").between(minZi, maxZi))
      .join(
        cells.as("n"),
        col("n.x") === col("nx") && col("n.y") === col("ny") && col("n.z") === col("nz"),
        "left_outer"
      )
      .groupBy(col("c.x").as("x"), col("c.y").as("y"), col("c.z").as("z"))
      .agg(
        sum(coalesce(col("n.cnt").cast(DoubleType), lit(0.0))).as("sumW"),
        count(lit(1)).as("W") // number of valid neighbors in-bounds (<= 27)
      )

    // 8) Compute Gi* score:
    // Gi* = (sumW - mean * W) / ( s * sqrt( (N*W - W^2) / (N - 1) ) )
    val giDF = neighAgg.withColumn(
      "gi",
      (col("sumW") - lit(meanX) * col("W").cast(DoubleType)) /
        (lit(s) * sqrt((lit(N) * col("W").cast(DoubleType) - pow(col("W").cast(DoubleType), 2.0)) / (lit(N) - lit(1.0))))
    )

    // 9) Return top hotspots (sorted by Gi* descending)
    giDF
      .orderBy(col("gi").desc, col("x"), col("y"), col("z"))
      .select("x", "y", "z")
  }
}
