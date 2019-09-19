import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("spark-streaming-app").getOrCreate()
  val rootPath: String = Paths.get("").toAbsolutePath.toString

  val mobileDataSchema =
    new StructType()
      .add("id", StringType, nullable = false)
      .add("action", StringType, nullable = false)
      .add("ts", TimestampType, nullable = false)

  val mobileSSDF = spark.readStream.schema(mobileDataSchema).json(rootPath + "/resources/data/input")
  val actionCountDF = mobileSSDF.groupBy(window(col("ts"), "10 minutes"), col("action")).count
  val mobileConsoleSQ = actionCountDF.writeStream.format("console").option("truncate", "false").outputMode("complete").start()
  mobileConsoleSQ.awaitTermination()
}