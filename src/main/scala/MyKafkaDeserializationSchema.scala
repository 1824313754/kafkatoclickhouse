import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema

/**
 * 自定义kafka反序列化
 * 将kafka中的topic和offset添加到json中
 */
class MyKafkaDeserializationSchema(groupId:String) extends KafkaDeserializationSchema[String] {
  override def isEndOfStream(t: String): Boolean = {
    false
  }
  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
    //获取kafka中topic名称和offset
    val topic: String = consumerRecord.topic()
    //获取分区
    val partition: Int = consumerRecord.partition()
    //获取消费者组id
    val offset: Long = consumerRecord.offset()
    //将topic和offset添加到json中
    val jsonObject: JSONObject = JSON.parseObject(new String(consumerRecord.value()))
    jsonObject.put("topicName",topic)
    jsonObject.put("topicOffset",offset)
    jsonObject.put("topicPartition",partition)
    jsonObject.put("topicGroupId",groupId)
    //取cTime,截取前10位
    val cTime: String = jsonObject.getString("cTime")
    jsonObject.put("dayOfYear",cTime.substring(0, 10))
    jsonObject.toJSONString
  }

  override def getProducedType: TypeInformation[String] = {
    TypeInformation.of(classOf[String])
  }

}
