import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import utils.ClickouseUtils

import java.lang
import java.nio.charset.StandardCharsets
import scala.io.Source.fromFile

/**
 * 批量生成insert into语句
 * @param properties
 */
class MyWindowFunction(properties: ParameterTool) extends ProcessWindowFunction[String,String,String,GlobalWindow ] {
  private val clickHouseUtil: ClickouseUtils = new ClickouseUtils(properties)
  //创建kafka的topic名称和clickhouse的表名的映射关系
  private var topicCkTable: Map[String, String]=_
  // 获取表的字段类型
  private var columnMap: Map[String,Map[String,String]] = _
  override def open(parameters: Configuration): Unit = {
    clickHouseUtil.connect()
    columnMap = clickHouseUtil.getColumnNames()
    //读取gpsPath中的数据
    val kafkatock = getRuntimeContext.getDistributedCache.getFile("kafkatock")
    val lines = fromFile(kafkatock, StandardCharsets.UTF_8.toString).getLines()
    topicCkTable = lines.map(line => {
      val splits = line.split(",")
      (splits(0), splits(1))
    }).toMap
  }

  override def process(key: String, context: ProcessWindowFunction[String, String, String, GlobalWindow]#Context, elements: lang.Iterable[String], out: Collector[String]): Unit = {
    val tableName=topicCkTable.get(key).get
    //获取当前表的字段和类型
    val map = columnMap.get(tableName).get
    val batchSql= clickHouseUtil.formatInsertSql(elements, tableName, map)
    out.collect(batchSql)
  }

}