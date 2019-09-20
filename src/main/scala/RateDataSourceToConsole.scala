import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object RateDataSourceToConsole extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("spark-streaming-app").getOrCreate()

  val rateSourceDF: DataFrame = spark.readStream.format("rate").option("rowsPerSecond", "5").load()
  val rateQuery: StreamingQuery = rateSourceDF.writeStream.outputMode("update").format("console").option("truncate", "false").start()
  rateQuery.awaitTermination()
}