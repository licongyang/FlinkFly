package org.flink.fly.process

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.flink.fly.source.SensorReading

// 温度低于某个值，低温报警
object MySideOutput {

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

    //    dataStream.print("data stream")

    val processDataStream = dataStream
      // 不用在keyby之后，
      // 传感器温度是华氏度 32度= 摄氏度0度
      // 低于32度，冰点报警
      .process(new LowTempAlert())

    processDataStream.print("main stream")

    processDataStream.getSideOutput(new OutputTag[String]("freezing alert")).print("side output stream")

    env.execute("side output ")


  }

  // 冰点报警，如果小雨32f,输出报警信息到侧输出流
  // 这里的输出，是主的流输出
  class LowTempAlert() extends ProcessFunction[SensorReading, SensorReading] {

    lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        // 侧输出流
        // 1.标签 2.输出值
        ctx.output(alertOutput, "freezing alert for " + value.id)
      } else {
        // 主流
        out.collect(value)
      }

    }
  }
}
