package test

import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

/**
 * 自定义kafka反序列化
 * 将kafka中的topic和offset添加到json中
 */
class MyKafka() extends KafkaDeserializationSchema[String] {
  //定义日志
  override def isEndOfStream(t: String): Boolean = {
    false
  }
  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
    //获取kafka中topic名称和offset
    val topic: String = consumerRecord.topic()
    //获取时间戳
    val timestamp: Long = consumerRecord.timestamp()
    //获取分区
    val partition: Int = consumerRecord.partition()
    //获取消费者组id
    val offset: Long = consumerRecord.offset()
    //将topic和offset添加到json中
    val jsonObject: JSONObject = JSON.parseObject(new String(consumerRecord.value()))
    jsonObject.put("topicName",topic)
    jsonObject.put("topicOffset",offset)
    jsonObject.put("topicPartition",partition)

//    val timestamp2 = jsonObject.getLong("timestamp")
    val cTime = jsonObject.getString("cTime")
    if (cTime==null) {
      println(jsonObject)
    }
    jsonObject.toJSONString
  }

  override def getProducedType: TypeInformation[String] = {
    TypeInformation.of(classOf[String])
  }

  /**
   * 检查json中是否包含必要字段
   * @param jsonObjectStr
   * @param fields
   * @return
   */
  def checkAndProcess(jsonObjectStr: String, fields: Array[String]) = {
    // 将JSON字符串解析为JSONObject
    val jsonObject = JSON.parseObject(jsonObjectStr)
    // 检查每个字段是否存在于JSON对象中
    val missingFields = fields.filter(field => !jsonObject.containsKey(field))
    if (missingFields.nonEmpty) {
      // 打印脏数据消息
      println("脏数据：" + jsonObject.toJSONString)
      null
    } else {
      // 所有字段都存在，可以继续处理JSON对象
      jsonObject
    }
  }

}
