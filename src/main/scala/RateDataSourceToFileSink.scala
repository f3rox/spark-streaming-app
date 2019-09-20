import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object RateDataSourceToFileSink extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("spark-streaming-app").getOrCreate()
  val rootPath: String = Paths.get("").toAbsolutePath.toString

  val rateSourceDF: DataFrame = spark.readStream.format("rate")
    .option("rowsPerSecond", "10")
    .option("numPartitions", "2")
    .load()
  val rateSQ: StreamingQuery = rateSourceDF.writeStream.outputMode("append")
    .format("json")
    .option("path", rootPath + "/tmp/output")
    .option("checkpointLocation", rootPath + "/tmp/ss/cp")
    .start()
  rateSQ.awaitTermination()
}