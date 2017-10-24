name := "raster-frames-template"

organization := "$package$"

scalaVersion := "2.11.11"

resolvers += Resolver.jcenterRepo

libraryDependencies ++= Seq(
  "io.astraea" %% "raster-frames" % "$rasterframes_version$",
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "$geotrellis_version$",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "$geotrellis_version$",
  "org.apache.spark" %% "spark-sql" % "$spark_version$"
)

// This is just for testing the template, and can be removed
test in Test := (runMain in Compile).toTask(" $package$.RasterFramesExample").value

// For running `sbt console` to get a Spark context with RasterFrames pre-initialized
initialCommands in console := """
import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName("RasterFrames")
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
rfInit(spark.sqlContext)
import spark.implicits._
"""

cleanupCommands in console := """
spark.stop()
"""
