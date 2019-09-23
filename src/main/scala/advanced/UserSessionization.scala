package advanced

import java.nio.file.Paths
import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

object UserSessionization extends App {

  case class UserActivity(user: String, action: String, page: String, ts: Timestamp)

  case class UserSessionState(var user: String, var status: String, var startTS: Timestamp, var endTS: Timestamp, var lastTS: Timestamp, var numPage: Int)

  case class UserSessionInfo(userId: String, start: Timestamp, end: Timestamp, numPage: Int)

  def updateUserActivity(userSessionState: UserSessionState, userActivity: UserActivity): UserSessionState = {
    userActivity.action match {
      case "login" =>
        userSessionState.startTS = userActivity.ts
        userSessionState.status = "Online"
      case "logout" =>
        userSessionState.endTS = userActivity.ts
        userSessionState.status = "Offline"
      case _ =>
        userSessionState.numPage += 1
        userSessionState.status = "Active"
    }
    userSessionState.lastTS = userActivity.ts
    userSessionState
  }

  def updateAcrossAllUserActivities(user: String, inputs: Iterator[UserActivity], oldState: GroupState[UserSessionState]): Iterator[UserSessionInfo] = {
    var userSessionState: UserSessionState =
      if (oldState.exists) oldState.get
      else UserSessionState(user, "", new Timestamp(System.currentTimeMillis), null, null, 0)
    var output: ListBuffer[UserSessionInfo] = ListBuffer[UserSessionInfo]()
    inputs.toList.sortBy(_.ts.getTime).foreach(userActivity => {
      userSessionState = updateUserActivity(userSessionState, userActivity)
      oldState.update(userSessionState)
      if (userActivity.action == "login")
        output += UserSessionInfo(user, userSessionState.startTS, userSessionState.endTS, 0)
    })
    val sessionTimedOut: Boolean = oldState.hasTimedOut
    val sessionEnded: Boolean = Option(userSessionState.endTS).isDefined
    val shouldOutput: Boolean = sessionTimedOut || sessionEnded
    shouldOutput match {
      case true =>
        if (sessionTimedOut) userSessionState.endTS = new Timestamp(oldState.getCurrentWatermarkMs)
        oldState.remove()
        output += UserSessionInfo(user, userSessionState.startTS, userSessionState.endTS, userSessionState.numPage)
      case _ =>
        oldState.update(userSessionState)
        oldState.setTimeoutTimestamp(userSessionState.lastTS.getTime, "30 minutes")
    }
    output.iterator
  }

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("spark-streaming-app").getOrCreate()
  val rootPath: String = Paths.get("").toAbsolutePath.toString

  import spark.implicits._

  val userActivitySchema: StructType = new StructType()
    .add("user", StringType, nullable = false)
    .add("action", StringType, nullable = false)
    .add("page", StringType, nullable = false)
    .add("ts", TimestampType, nullable = false)

  val userActivityDF: DataFrame = spark.readStream.schema(userActivitySchema).json(rootPath + "/resources/data/sessionization/input")
  val userActivityDS: Dataset[UserActivity] = userActivityDF.withWatermark("ts", "30 minutes").as[UserActivity]
  val userSessionDS: Dataset[UserSessionInfo] = userActivityDS.groupByKey(_.user)
    .flatMapGroupsWithState[UserSessionState, UserSessionInfo](OutputMode.Append, GroupStateTimeout.EventTimeTimeout())(updateAcrossAllUserActivities)
  val userSessionSQ: StreamingQuery = userSessionDS.writeStream.format("console").option("truncate", value = false).outputMode("append").start()
  userSessionSQ.awaitTermination()
}