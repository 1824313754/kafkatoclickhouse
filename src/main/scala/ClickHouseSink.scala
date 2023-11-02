package sink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import utils.ClickouseUtils

/**
 * 写入clickhouse，失败可重连
 * @param properties
 * @param <T>
 *
 */


class ClickHouseSink(properties: ParameterTool) extends RichSinkFunction[String] {
  private val clickouseUtils = new ClickouseUtils(properties)
  override def open(parameters: Configuration): Unit = {
    clickouseUtils.connect()
  }

  override def invoke(sql: String, context: Context): Unit = {
    clickouseUtils.executeInsertSql(sql)
  }

  override def close(): Unit = {
    clickouseUtils.close()
  }

}
