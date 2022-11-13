package org.flink.fly.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.flink.fly.source.SensorReading

import java.util.Properties

// 如何端到端状态一致性
object MyKafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    val inputstream = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/src/main/resources/sensor.txt")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    偏移量重置 最近位置
    properties.setProperty("auto.offset.reset", "latest")


    val inputstream = env.addSource(new FlinkKafkaConsumer011[String](
      "sensor", new SimpleStringSchema(), properties))

    val dataStream = inputstream.map(data => {
      val dataArray = data.split(",")
      // 转成string方便序列化输出
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
    })

     dataStream.print("data stream")

    // sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "sinkTest", new SimpleStringSchema()))


    env.execute("kafka sink ")
  }

}
