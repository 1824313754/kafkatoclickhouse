package utils

import org.apache.flink.api.java.utils.ParameterTool

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Properties

object GetConfig extends Serializable {
//  val gps_schema = StructType(List(StructField("province",StringType),StructField("city",StringType),StructField("area",StringType),StructField("lat",DoubleType),StructField("lon",DoubleType),StructField("region",StringType)))

  def getProperties(filename :String)={
    val parameterTool: ParameterTool = ParameterTool.fromPropertiesFile(filename)
    parameterTool
  }
  def getTimeStr(): String = {
    // 获取当前时间
    val now = System.currentTimeMillis()
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(now)
  }
  def createConsumerProperties(properties:ParameterTool): java.util.Properties = {
    val props = new java.util.Properties()
    //从配置文件中读取
    props.setProperty("bootstrap.servers", properties.get("kafka.bootstrap.servers"))
    props.setProperty("group.id", properties.get("kafka.consumer.groupid"))
    props.setProperty("auto.offset.reset", properties.get("kafka.auto.offset.reset"))
    props.setProperty("enable.auto.commit", properties.get("kafka.enable.auto.commit"))
    props.setProperty("auto.offset.reset", properties.get("kafka.auto.offset.reset"))
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

}
