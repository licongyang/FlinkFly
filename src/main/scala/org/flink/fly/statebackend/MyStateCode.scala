package org.flink.fly.statebackend

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.flink.fly.source.SensorReading

// 温度低于某个值，低温报警
object MyStateCode {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 开启checkpoint,每分钟一次

    /**
     *
     *     def enableCheckpointing(interval : Long,
     *                       mode: CheckpointingMode) : StreamExecutionEnvironment = {
     */
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointInterval(60000)
    // 默认 状态一致性为exact_once
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 保存状态后端（io），超过这个事件，这个checkpoint就丢弃 ；单位毫秒
    env.getCheckpointConfig.setCheckpointTimeout(100000)
    // checkpoint报错是否把job fail; 默认为true
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 同时进行几个checkpoint，间隔比较小，一个还没完，另一个又来；性能可能受影响
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 两次checkpoint的最小事件间隔；跟上面冲突，只能一个生效
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    // 开启checkpoint外部持久化； job fail, 外部checkpoint会被清理；即使job失败，外部checkpoint不会被清理，需要手工清理
    // job手工取消的，不要存 ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION
    // ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION 需要存
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 重启策略 1.yaml文件参数
    // 故障失败之后，尝试重启三次，两次重启间隔500ms
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 500))
//    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.seconds(300), org.apache.flink.api.common.time.Time.seconds(10)))
    // 配置statebackend，保存checkpoint
    //    env.setStateBackend(new MemoryStateBackend())
    //    env.setStateBackend(new FsStateBackend("hdfs://"))
    //    env.setStateBackend(new RocksDBStateBackend("checkpointpath"))

    val stream = env.socketTextStream("localhost", 7778)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })


    // 检测两次温度变化，超过一定范围，就报警（不能跳变太大），检测某一个传感器（keyby）
    val processDataStream1 = dataStream.keyBy(_.id)
      // 超过10f就报警
      .process(new TempChangeAlert(10.0))
    // 检测两次温度变化，超过一定范围，就报警（不能跳变太大），检测某一个传感器（keyby）
    val processDataStream2 = dataStream.keyBy(_.id)
      // processfunction，事件事件、底层东西都可以拿到；但这里操作简单，只是简单判断，转换输出以下。可以用map\flatmap
      .flatMap(new TempChangeAlertWithFlatMap(10.0))

    //    processDataStream.print("alert data1")

    // 简便快速带状态
    val processDataStream3 = dataStream.keyBy(_.id)

      /**
       * def flatMapWithState[R: TypeInformation, S: TypeInformation](
       * // 输出 R：TypeInformation 定义的状态类型 S:TypeInformation
       * // Option：有可能开始没有状态，判断状态是否没有
       * fun: (T, Option[S]) => (TraversableOnce[R], Option[S])): DataStream[R] = {
       * 来了一个输入，输出TraversableOnce[R](可序列化一次数据类型，list一个列表)，和状态变化
       * 状态管理时，输入数据和状态；输出结果和状态
       */
      .flatMapWithState[(String, Double, Double), Double] {
        // 如果没有状态的话，也就是没有数据来过，那么就将当前数据温度存入状态
        case (input: SensorReading, None) => (List.empty, Some(input.temperature))
        // 如果有状态的话，就应该与上次的温度值比较差值，如果大于阀值就输出报警
        case (input: SensorReading, lastTemp: Some[Double]) =>
          val diff = (input.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
          } else
            (List.empty, Some(input.temperature))
      }
    dataStream.print("data stream")
    // 包含第一条数据的（指定状态为none）处理，不会报警，
    processDataStream3.print("process data3")

    env.execute("temp change too big to alert ")


  }

  // 输出：id，两次温度
  class TempChangeAlert(threadhold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
    // 定义一个状态变量，保存上次的温度值
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
      // 获取上次的温度值
      val lastTemp = lastTempState.value()
      // 用当前温度值和上次的求差，如果大于阀值，输出报警信息
      val diff = (value.temperature - lastTemp).abs
      if (diff > threadhold) {
        out.collect(value.id, lastTemp, value.temperature)
      }
      // 更新上次温度状态
      lastTempState.update(value.temperature)
    }
  }

  class TempChangeAlertWithFlatMap(threadhold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

    private var lastTempState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      // 初始化的时候声明state变量
      lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
    }

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      // 获取上次的温度值
      val lastTemp = lastTempState.value()
      // 用当前温度值和上次的求差，如果大于阀值，输出报警信息
      val diff = (value.temperature - lastTemp).abs
      if (diff > threadhold) {
        out.collect(value.id, lastTemp, value.temperature)
      }
      // 更新上次温度状态
      lastTempState.update(value.temperature)

    }


  }
}
