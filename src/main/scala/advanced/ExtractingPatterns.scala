package advanced

import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQuery}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ExtractingPatterns extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("spark-streaming-app").getOrCreate()
  val rootPath: String = Paths.get("").toAbsolutePath.toString

  import spark.sqlContext.implicits._

  case class RackInfo(rack: String, temperature: Double, ts: java.sql.Timestamp)

  case class RackState(var rackId: String, var highTempCount: Int, var status: String, var lastTS: java.sql.Timestamp)

  def updateRackState(rackState: RackState, rackInfo: RackInfo): RackState = {
    val lastTS = Option(rackState.lastTS).getOrElse(rackInfo.ts)
    val withinTimeThreshold = (rackInfo.ts.getTime - lastTS.getTime) <= 60000
    val meetCondition =
      if (rackState.highTempCount < 1) true
      else withinTimeThreshold
    val greaterThanEqualTo100 = rackInfo.temperature >= 100.0
    (greaterThanEqualTo100, meetCondition) match {
      case (true, true) =>
        rackState.highTempCount += 1
        rackState.status =
          if (rackState.highTempCount >= 3) "Warning"
          else "Normal"
      case _ =>
        rackState.highTempCount = 0
        rackState.status = "Normal"
    }
    rackState.lastTS = rackInfo.ts
    rackState
  }

  def updateAcrossAllRackStatus(rackId: String, inputs: Iterator[RackInfo], oldState: GroupState[RackState]): RackState = {
    var rackState =
      if (oldState.exists) oldState.get
      else RackState(rackId, 5, "", null)
    inputs.toList.sortBy(_.ts.getTime).foreach(input => {
      rackState = updateRackState(rackState, input)
      oldState.update(rackState)
    })
    rackState
  }

  val iotDataSchema: StructType = new StructType()
    .add("rack", StringType, nullable = false)
    .add("temperature", DoubleType, nullable = false)
    .add("ts", TimestampType, nullable = false)

  val iotSSDF: DataFrame = spark.readStream.schema(iotDataSchema).json(rootPath + "/resources/data/iot_pattern/input")
  val iotPatternDF: Dataset[RackState] = iotSSDF.as[RackInfo].groupByKey(_.rack).mapGroupsWithState[RackState, RackState](GroupStateTimeout.NoTimeout)(updateAcrossAllRackStatus)
  val iotPatternSQ: StreamingQuery = iotPatternDF.writeStream.format("console").outputMode("update").start()
  iotPatternSQ.awaitTermination()
}