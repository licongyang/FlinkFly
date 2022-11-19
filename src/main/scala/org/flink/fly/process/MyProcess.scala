package org.flink.fly.process

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.flink.fly.source.SensorReading

// 10s温度连续上升，输出数据id,报警信息
object MyProcess {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val stream = env.socketTextStream("localhost", 7777)
    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })
    dataStream.print("data stream")
    // 一段温度连续上升，开个窗口不好使；可以考虑processfunction
    val processedDataStream = dataStream.keyBy(_.id)
      .process(new TempIncreaseAlert())

    processedDataStream.print("process data")
    env.execute("process warning")
  }

  // id， 输入，输出
  // 判断温度连续上升（跟上次温度比较）
  // 报警，可以通过定时器
  // 来一个数据就调用该方法
  class TempIncreaseAlert() extends KeyedProcessFunction[String, SensorReading, String]{
    // 上一次数据温度值，保存成状态
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
    // 定义一个状态，用来保存定时器的时间戳
    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer",classOf[Long]))
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      // 先取出上一个温度值
      val preTemp = lastTemp.value()
      // 更新温度值
      lastTemp.update(value.temperature)
      // 没有注册过注册器，则默认0
      val currentTimerTs = currentTimer.value()

      // 温度上升，则注册定时器 ;且没有注册过定时器
      if(value.temperature > preTemp && currentTimerTs == 0){
        // 参数是一个时间戳，不是延迟。当前时间+1s
        val timerTs = ctx.timerService().currentProcessingTime() + 10000L
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        currentTimer.update(timerTs)
      }else if( value.temperature < preTemp || preTemp == 0.0){
        // 温度不上升，需要删除定时器; 第一条数据，有定时器就删除
        // 注册定时器是根据当时时间定义的，那么删除需要传入当时的时间戳
        ctx.timerService().deleteProcessingTimeTimer(currentTimerTs)
        // 清空定时器定义时间戳
        currentTimer.clear()
      }
    }
    // 定时器回调处理
    // timestamp当时设定的时间，当前时间
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 1s内没有清掉，就直接输出报警信息
      out.collect("sensor_" +  ctx.getCurrentKey + "温度连续上升")
      //清空状态
      currentTimer.clear()
    }
  }
}
