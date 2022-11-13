package org.flink.fly.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties
import scala.util.Random

// 温度传感器读数样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object MySource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    1. 从自定义的集合中读取数据
    val streamFromCollection = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    streamFromCollection.print("collect source").setParallelism(6)

    env.fromElements(1, 2.0, "String").print("stream from element")

    //    2. socket
    //    3. 文件流
    val stream2 = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/src/main/resources/sensor.txt")

    stream2.print("file source").setParallelism(1)

    //    4. 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    偏移量重置 最近位置
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource(new FlinkKafkaConsumer011[String](
      "sensor", new SimpleStringSchema(), properties))

    stream3.print("kafka source").setParallelism(1)

    //    自定义source，适用于测试场景
    val stream4 = env.addSource(new SensorSource())
    stream4.print("custom source")


    env.execute("source teest")
  }

  class SensorSource() extends SourceFunction[SensorReading] {

    // 定义一个flag， 表示数据源是否正常运行
    var running: Boolean = true

    // 正常生成数据
    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
      // 初始化一个随机数发生器
      val random = new Random()

      // 隔一段时间，就生成一批数据；每次数据温度基于上次数据温度基础稍微变化，叠加变化
      // 初始化定义一组传感器温度数据
      var curTemp = 1.to(10).map(
        // 基于温度60 ，加上个高斯分布（正态分布）
        i => ("sensor_" + i, 60 + random.nextGaussian() * 20)
      )

      // 用无限循环，产生数据流
      while(running){
        // 在前一次温度的基础上更新温度值
        curTemp = curTemp.map(
          t => (t._1, t._2 + random.nextGaussian())
        )
        // 获取当前时间戳
        val curTime = System.currentTimeMillis()
        curTemp.foreach(
          // 封装传感器对象
          // 对象数据通过上下文发送出去
          t => sourceContext.collect(SensorReading(t._1, curTime / 1000, t._2))
        )
        // 等待以下，设置时间间隔
        Thread.sleep(1000)
      }
    }

    // 取消数据源的生成
    override def cancel(): Unit = {
      running = false
    }
  }
}
