package spatial.analytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HotzoneAnalysis {

  private def configureLogging(): Unit = {
    Seq("org.spark_project", "org.apache", "akka", "com").foreach { name =>
      Logger.getLogger(name).setLevel(Level.WARN)
    }
  }

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {
    configureLogging()

    // Read point dataset (semicolon delimited) and extract the pickup point string column.
    val rawPoints = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "false")
      .load(pointPath)

    // Use a UDF to remove parentheses from "(x,y)" strings.
    val stripParens = udf { s: String =>
      if (s == null) null else s.replace("(", "").replace(")", "")
    }

    val pointStrings = rawPoints
      .select(stripParens(col("_c5")).as("pointString"))
      .where(col("pointString").isNotNull)

    // Read rectangles dataset (tab delimited)
    val rectangles = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(rectanglePath)

    // Register spatial predicate (delegates to HotzoneUtils)
    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) => HotzoneUtils.ST_Contains(queryRectangle, pointString)
    )

    // Join rectangles with points that fall inside them, then aggregate counts per rectangle
    rectangles.createOrReplaceTempView("rectangle")
    pointStrings.createOrReplaceTempView("point_strings")

    val resultDf = spark.sql(
      """
        |SELECT r._c0 AS rectangle, COUNT(*) AS count
        |FROM rectangle r, point_strings p
        |WHERE ST_Contains(r._c0, p.pointString)
        |GROUP BY r._c0
        |ORDER BY r._c0 ASC
      """.stripMargin
    )

    resultDf
  }
}
