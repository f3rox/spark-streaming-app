import org.apache.spark.sql.{ForeachWriter, Row}

class ConsoleWriter(private var pId: Long = 0, private var ver: Long = 0) extends ForeachWriter[Row] {
  override def open(partitionId: Long, version: Long): Boolean = {
    pId = partitionId
    ver = version
    println(s"open => ($partitionId, $version)")
    true
  }

  override def process(row: Row): Unit = println(s"process => $row")

  override def close(errorOrNull: Throwable): Unit = println(s"close => ($pId, $ver)")
}