import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SocketDataSourceToConsole extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("spark-streaming-app").getOrCreate()

  import spark.sqlContext.implicits._

  val socketDF: DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()
  val words: Dataset[String] = socketDF.as[String].flatMap(_.split(" "))
  val wordCounts: DataFrame = words.groupBy("value").count
  val query: StreamingQuery = wordCounts.writeStream.format("console").outputMode("complete").start()
  query.awaitTermination()
}