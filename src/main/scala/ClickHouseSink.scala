package sink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection}
import ru.yandex.clickhouse.settings.ClickHouseProperties

/**
 * 写入clickhouse，失败可重连
 * @param properties
 * @param <T>
 *
 */


class ClickHouseSink(properties: ParameterTool) extends RichSinkFunction[String] {
  private var connection: ClickHouseConnection = _
  private val maxRetries: Int = properties.getInt("clickhouse.maxRetries", 3)

  override def open(parameters: Configuration): Unit = {
    connect()
  }

  override def invoke(sql: String, context: Context): Unit = {
    retryOnFailure(maxRetries) {
      val statement = connection.createStatement()
      statement.executeUpdate(sql)
    }
  }

  override def close(): Unit = {
    connection.close()
  }

  private def connect(): Unit = {
    retryOnFailure(maxRetries) {
      val clickPro = new ClickHouseProperties()
      clickPro.setUser(properties.get("clickhouse.user"))
      clickPro.setPassword(properties.get("clickhouse.passwd"))
      val source = new BalancedClickhouseDataSource(properties.get("clickhouse.conn"), clickPro)
      source.actualize()
      connection = source.getConnection
    }
  }

  //重试机制
  private def retryOnFailure(maxRetries: Int)(block: => Unit): Unit = {
    var retries = 0
    var success = false
    while (!success && retries < maxRetries) {
      try {
        block //执行代码块
        success = true
      } catch {
        case e: Exception =>
          retries += 1
          if (retries < maxRetries) {
            println(s"Operation failed, retrying ($retries/$maxRetries). ${e.getMessage}")
//            connect() //重连
          } else {
            println(s"Max retries exceeded. Failed to perform the operation.")
            e.printStackTrace()
          }
      }
    }
  }
}
