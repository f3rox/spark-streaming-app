import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RateDataSourceToCustomWriter extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession = SparkSession.builder().master("local[1]").appName("spark-streaming-app").getOrCreate()

  val ratesSourceDF = spark.readStream.format("rate").option("rowsPerSecond", "10").option("numPartitions", "2").load()
  val rateSQ = ratesSourceDF.writeStream.foreach(new ConsoleWriter).start()
  rateSQ.awaitTermination()
}