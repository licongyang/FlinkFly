package org.flink.fly.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

// 温度传感器读数样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    1. 从自定义的集合中读取数据
    val streamFromCollection = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    streamFromCollection.print("stream1").setParallelism(6)

    env.fromElements(1, 2.0, "String").print("stream from element")

    //    2. socket
    //    3. 文件流
    val stream2 = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/src/main/resources/sensor.txt")

    stream2.print("stream2").setParallelism(1)

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

    stream3.print("stream3").setParallelism(1)


    env.execute("source teest")
  }
}
