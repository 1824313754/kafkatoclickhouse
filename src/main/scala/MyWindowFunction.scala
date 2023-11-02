import com.alibaba.fastjson.JSON
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import ru.yandex.clickhouse.settings.ClickHouseProperties
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection, ClickHouseUtil}

import java.lang
import java.nio.charset.StandardCharsets
import scala.io.Source.fromFile

/**
 * 批量生成insert into语句
 * @param properties
 */
class MyWindowFunction(properties: ParameterTool) extends ProcessWindowFunction[String,String,String,GlobalWindow ] {
  private var connection: ClickHouseConnection = _
  private val database: String = properties.get("clickhouse.database")
  //创建kafka的topic名称和clickhouse的表名的映射关系
  private var topicCkTable: Map[String, String]=_

  // 获取表的字段类型
  private var columnMap: Map[String,Map[String,String]] = _
  //保留引号的类型
  private val stringTypes: Set[String] = properties.get("clickhouse.stringTypes").split(",").map(_.toLowerCase).toSet

  override def open(parameters: Configuration): Unit = {
    connect()
    columnMap = getColumnNames()
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
    val batchSql = new StringBuilder(s"insert into $database.$tableName (")
    val batchValues = new StringBuilder("values")
    val elementsIterator = elements.iterator()
    while (elementsIterator.hasNext) {
      val essInfo = elementsIterator.next()
      val essInfoJson = JSON.parseObject(essInfo)
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

  def getColumnNames(): Map[String, Map[String, String]] = {
    val sql = s"select `table`,name, `type`, table from system.columns where database='$database'"
    val rs = connection.createStatement().executeQuery(sql)

    var resultMap = Map[String, Map[String, String]]()
    while (rs.next()) {
      val tableName = rs.getString("table")
      val columnName = rs.getString("name")
      val columnType = rs.getString("type")
      val innerMap = resultMap.getOrElse(tableName, Map[String, String]())
      val updatedInnerMap = innerMap + (columnName -> columnType)
      resultMap = resultMap + (tableName -> updatedInnerMap)
    }
    resultMap
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