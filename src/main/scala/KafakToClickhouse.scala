import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import sink.ClickHouseSink
import utils.GetConfig
import utils.GetConfig.createConsumerProperties

import scala.collection.JavaConverters.bufferAsJavaListConverter

object KafakToClickhouse {
  def main(args: Array[String]): Unit = {
    //定义flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val fileName: String = tool.get("config_path")
    val properties = GetConfig.getProperties(fileName)
    //获取当前环境
    properties.get("flink.env") match {
      case "test" => env.setParallelism(1)
      case _ =>
        //设置checkpoint
        env.enableCheckpointing(properties.get("checkpoint.interval").toInt)
        //设置重启策略，3次重启，每次间隔5秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(properties.getInt("restart.num"), properties.getLong("restart.interval")))
        //设置最大checkpoint并行度
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        //设置checkpoint超时时间
        env.getCheckpointConfig.setCheckpointTimeout(properties.getLong("checkpoint.timeout"))
        //设置RocksDBStateBackend,增量快照
        env.setStateBackend(new FsStateBackend(properties.get("checkpoint.path"), true))
        //设置任务取消时保留checkpoint
        env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }
    //注册为全局变量
    env.getConfig.setGlobalJobParameters(properties)

    val topicString = properties.get("kafka.topic")
    //消费者组id
    val groupId = properties.get("kafka.consumer.groupid")
    val topicList: java.util.List[String] = topicString.split(",").toBuffer.asJava
    //获取要检测的必要字段
    val checkField: Array[String] = properties.get("check.field").split(",")
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(
      topicList,
      new MyKafkaDeserializationSchema(groupId,checkField),
      createConsumerProperties(properties)
    )
    val dataStream: DataStream[String] = env.addSource(kafkaConsumer).uid("kafkaSource").name("kafkaSource")
      .filter(_ != null).uid("filter").name("filter")

    // 添加窗口函数逻辑
    val windowSize = properties.getInt("window.size", 20)
    val outputStream: DataStream[String] = dataStream
      .keyBy(JSON.parseObject(_).getString("essCode"))
      .countWindow(windowSize)
      .process(new MyWindowFunction(properties)).uid("windowFunction").name("windowFunction")
    outputStream.addSink(new ClickHouseSink(properties)).uid("clickhouseSink").name("clickhouseSink")
    env.execute("KaflaToClickhouse")
  }

}
