import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.IterationRuntimeContext
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection, ClickHouseUtil}
import ru.yandex.clickhouse.settings.ClickHouseProperties
import utils.GetConfig.getTimeStr

import java.lang

/**
 * 批量生成insert into语句
 * @param properties
 */
class MyWindowFunction(properties: ParameterTool) extends ProcessWindowFunction[String,String,String,TimeWindow] {
  private var connection: ClickHouseConnection = _
  private val database: String = properties.get("clickhouse.database")
  private val tableName: String = properties.get("clickhouse.table")

  // 获取表的字段类型
  private var map: Map[String, String] = _
  //保留引号的类型
  private val stringTypes: Set[String] = properties.get("clickhouse.stringTypes").split(",").map(_.toLowerCase).toSet

  override def open(parameters: Configuration): Unit = {
    connect()
    map = getCoumnName()
  }

  override def process(key: String, context: ProcessWindowFunction[String, String, String, TimeWindow]#Context, elements: lang.Iterable[String], out: Collector[String]): Unit = {
    val batchSql = new StringBuilder(s"insert into $database.$tableName (")
    val batchValues = new StringBuilder("values ")
    val elementsIterator = elements.iterator()
    while (elementsIterator.hasNext) {
//      println(elementsIterator.hasNext)
      val essInfo = elementsIterator.next()
      val essInfoJson = JSON.parseObject(essInfo)
      essInfoJson.put("dayOfYear", essInfoJson.getString("cTime").substring(0, 10))
      //当前时间转为yyyy-MM-dd HH:mm:ss
      essInfoJson.put("sTime", getTimeStr)
      val values = new StringBuilder("(")
      for ((key, clickHouseType) <- map) {
        val value = essInfoJson.getString(key)
        values.append(formatValue(value, clickHouseType) + ",")
      }
      values.deleteCharAt(values.length - 1)
      values.append("),")
      batchValues.append(values)
      }
    batchValues.deleteCharAt(batchValues.length - 1)
    batchSql.append(map.keys.mkString(",")).append(") ")
    batchSql.append(batchValues)
    out.collect(batchSql.toString())
  }

  override def close(): Unit = {
    connection.close()
  }

  private def connect(): Unit = {
    val clickPro = new ClickHouseProperties()
    clickPro.setUser(properties.get("clickhouse.user"))
    clickPro.setPassword(properties.get("clickhouse.passwd"))
    val source = new BalancedClickhouseDataSource(properties.get("clickhouse.conn"), clickPro)
    source.actualize()
    connection = source.getConnection
  }

  def getCoumnName() = {
    val sql = s"select name,`type`  from system.columns where database='$database' and  table='$tableName'"
    val rs = connection.createStatement().executeQuery(sql)
    // Define a map
    var map = Map[String, String]()
    while (rs.next()) {
      val coumnName = rs.getString("name")
      val coumnType = rs.getString("type")
      map += (coumnName -> coumnType)
    }
    map
  }

  // 根据字段类型格式化字段值
  private def formatValue(value: String, clickHouseType: String): Any = {
    if (value == null || value.isEmpty) {
      return null
    }
    val lowerCaseType = clickHouseType.toLowerCase
    if (stringTypes.contains(lowerCaseType)) {
      s"'${ClickHouseUtil.escape(value)}'"
    } else {
      value
    }
  }

}