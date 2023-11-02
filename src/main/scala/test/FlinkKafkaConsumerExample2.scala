package test

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Properties

object FlinkKafkaConsumerExample2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "192.168.10.3:9092,192.168.10.3:9093,192.168.10.3:9094")
    kafkaProps.setProperty("group.id", "test-group-9-2-test")
    val topic = "ess-ods-cluster-03"
    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), kafkaProps)

    // 指定从特定时间开始消费,时间格式为: 2020-01-01 00:00:00
    val timestampInMillis = LocalDateTime.parse("2023-10-06 23:56:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(ZoneOffset.of("+8")).toEpochMilli

    consumer.setStartFromTimestamp(timestampInMillis)

    env.addSource(consumer).addSink(new PrintSinkFunction[String](){
      override def invoke(record: String): Unit = {
        val nObject = JSON.parseObject(record)
        if (nObject.getIntValue("wareNum")==37 ){
          val ctime=nObject.getString("cTime")
          println(ctime,nObject)
        }
      }
    })

    env.execute("Flink Kafka Consumer Example")
  }
}
