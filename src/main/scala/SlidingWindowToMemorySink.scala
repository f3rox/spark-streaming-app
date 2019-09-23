import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SlidingWindowToMemorySink extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("spark-streaming-app").getOrCreate()
  val rootPath: String = Paths.get("").toAbsolutePath.toString

  val iotDataSchema: StructType = new StructType()
    .add("rack", StringType, nullable = false)
    .add("temperature", DoubleType, nullable = false)
    .add("ts", TimestampType, nullable = false)

  val iotSSDF: DataFrame = spark.readStream.schema(iotDataSchema).json(rootPath + "/resources/data/iot")
  val iotAvgByRackDF: DataFrame = iotSSDF.groupBy(window(col("ts"), "10 minutes", "5 minutes"), col("rack")).agg(avg("temperature") as "avg_temp")
  val iotMemorySQ: StreamingQuery = iotAvgByRackDF.writeStream.format("memory").queryName("iot_rack").outputMode("complete").start()
  iotMemorySQ.awaitTermination(5000)
  spark.sql("select * from iot_rack").orderBy(col("rack"), col("window.start")).show(truncate = false)
}