import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileDataSourceToConsole extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("spark-streaming-app").getOrCreate()
  val rootPath: String = Paths.get("").toAbsolutePath.toString

  val mobileDataSchema: StructType =
    new StructType()
      .add("id", StringType, nullable = false)
      .add("action", StringType, nullable = false)
      .add("ts", TimestampType, nullable = false)

  val mobileSSDF: DataFrame = spark.readStream.schema(mobileDataSchema).json(rootPath + "/resources/data/input")
  println(mobileSSDF.isStreaming)
  val cleanMobileSSDF: DataFrame = mobileSSDF
    .filter(col("action") === "open" || col("action") === "close")
    .select(col("id"), upper(col("action")), col("ts"))

  val actionCountDF: DataFrame = mobileSSDF.groupBy(window(col("ts"), "10 minutes"), col("action")).count
  val mobileConsoleSQ: StreamingQuery = actionCountDF.writeStream.format("console").option("truncate", "false").outputMode("complete").start()

  cleanMobileSSDF.createOrReplaceTempView("clean_mobile")
  println(cleanMobileSSDF.isStreaming)
  Thread.sleep(5000)
  spark.sql("select count(*) from clean_mobile").show()
  mobileConsoleSQ.awaitTermination()
}