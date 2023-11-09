package utils

import com.alibaba.fastjson.JSON
import org.apache.flink.api.java.utils.ParameterTool
import ru.yandex.clickhouse.settings.ClickHouseProperties
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection, ClickHouseUtil}

import java.sql.ResultSet
import scala.collection.JavaConversions.`deprecated iterableAsScalaIterable`

/**
 * @Author WangJie
 * @Date 2023/11/2 10:03 
 * @description: ${description}
 * @Title: ClickouseUtils
 * @Package utils 
 */
class ClickouseUtils(properties: ParameterTool) extends Serializable{
  private var connection: ClickHouseConnection = _
  private val maxRetries: Int = properties.getInt("clickhouse.maxRetries", 3)
  private val stringTypes: Set[String] = properties.get("clickhouse.stringTypes").split(",").map(_.toLowerCase).toSet
  private val database: String = properties.get("clickhouse.database")


  /**
   * 连接clickhouse
   * @return
   */
  def connect() = {
    retryOnFailure(maxRetries) {
      val clickPro = new ClickHouseProperties()
      clickPro.setUser(properties.get("clickhouse.user"))
      clickPro.setPassword(properties.get("clickhouse.passwd"))
      val source = new BalancedClickhouseDataSource(properties.get("clickhouse.conn"), clickPro)
      source.actualize()
      this.connection = source.getConnection
    }
  }

  /**
   * 执行sql
   * @param sql
   */
  def executeInsertSql(sql: String): Unit = {
    retryOnFailure(maxRetries) {
      val statement = connection.createStatement()
      statement.executeUpdate(sql)
    }
  }

  /**
   * 执行查询sql
   * @param sql
   * @return ResultSet
   */
  def executeQuerySql(sql: String)  = {
    var set:ResultSet = null
    retryOnFailure(maxRetries) {
      val statement = connection.createStatement()
       set = statement.executeQuery(sql)
    }
    set
  }

  /**
   * 关闭连接
   * @return
   */
  def close(): Unit = {
    connection.close()
  }

  /**
   * 格式化value
   * @param value
   * @param clickHouseType
   * @return
   */
   def formatValue(value: String, clickHouseType: String): Any = {
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

  /**
   * 根据json列表格式化sql
   * @param jsonList,tableName,columnMap
   * @return String
   */

  def formatInsertSql(jsonList:java.lang.Iterable[String], tableName: String, columnMap: Map[String, String]): String = {
    val batchSql = new StringBuilder(s"insert into $database.$tableName (")
    val batchValues = new StringBuilder("values")
    jsonList.foreach { json: String =>
      val values = new StringBuilder("(")
      val jsonMap = JSON.parseObject(json)
      for ((key, clickHouseType) <- columnMap) {
        val value = jsonMap.getString(key)
        values.append(formatValue(value, clickHouseType) + ",")
      }
      values.deleteCharAt(values.length - 1)
      values.append("),")
      batchValues.append(values)
    }


    batchValues.deleteCharAt(batchValues.length - 1)
    batchSql.append(columnMap.keys.mkString(",")).append(") ")
    batchSql.append(batchValues)
    batchSql.toString()
  }

  /**
   * 获取表的字段和类型对应关系
   * @param tableName
   */
  def getColumnNames(): Map[String, Map[String, String]] = {
    val sql = s"select `table`,name, `type`, table from system.columns where database='$database'"
    val rs = executeQuerySql(sql)
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
  /**
   * 重试
   * @param maxRetries
   * @param block
   */
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
          } else {
            println(s"Max retries exceeded. Failed to perform the operation.")
            e.printStackTrace()
          }
      }
    }
  }
}


