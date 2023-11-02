package utils

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.LoggerFactory

import java.sql.{Connection, SQLException}

object ConnectionPool extends Serializable {
   private val logger = LoggerFactory.getLogger(ConnectionPool.getClass)
  @volatile  private var connectionPool: BoneCP = _
  def getConnection(properties: ParameterTool): Connection = {
    if (connectionPool == null) {
      synchronized {
        if (connectionPool == null) {
          initConnectionPool(properties)
        }
      }
    }
    try {
      connectionPool.getConnection
    } catch {
      case e: SQLException =>
        logger.error("Failed to get a connection from the connection pool", e)
        throw e
    }
  }

  def closeConnection(connection: Connection): Unit = {
    try {
      if (connection != null && !connection.isClosed) {
        connection.close()
      }
    } catch {
      case e: SQLException =>
        logger.error("Failed to close the connection", e)
    }
  }

  private def initConnectionPool(properties: ParameterTool): Unit = {
    logger.info("Initializing connection pool")
    val config = new BoneCPConfig()
    config.setJdbcUrl(properties.get("mysql.conn"))
    config.setUsername(properties.get("mysql.user"))
    config.setPassword(properties.get("mysql.passwd"))
    config.setMinConnectionsPerPartition(1)
    config.setMaxConnectionsPerPartition(2)
    config.setPartitionCount(6)
    config.setCloseConnectionWatch(false)
    config.setLazyInit(true)
    connectionPool = new BoneCP(config)
    logger.info("Connection pool initialized successfully")
  }
}
