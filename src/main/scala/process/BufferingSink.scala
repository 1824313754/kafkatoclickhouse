package process

import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * @Author WangJie
 * @Date 2023/11/1 16:29 
 * @description: ${description}
 * @Title: BatchInsterProcess
 * @Package process 
 */
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class BufferingSink(threshold: Int) extends SinkFunction[String] with CheckpointedFunction {
  private var bufferedElements: List[String] = _
  private var checkpointedState: ListState[String] = _

  override def invoke(value: String, context: SinkFunction.Context): Unit = {
    bufferedElements :+= value
    if (bufferedElements.size == threshold) {
      for (event <- bufferedElements) {
      }
      println("====================输出完毕====================")
      bufferedElements = List[String]()
    }
  }



  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    for (event <- bufferedElements) {
      checkpointedState.add(event)
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[String]("buffer", classOf[String])
    checkpointedState = context.getOperatorStateStore.getListState(descriptor)
    if (context.isRestored) {
      bufferedElements = checkpointedState.get().asScala.toList
    }
  }
}

