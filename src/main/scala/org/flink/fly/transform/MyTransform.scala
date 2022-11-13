package org.flink.fly.transform

import org.apache.flink.streaming.api.scala._
import org.flink.fly.source.SensorReading

object MyTransform {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val streaFromFile = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/src/main/resources/sensor.txt")

    // map ：返回DataStream ,里面范型可以改变,将之前的String转换为SensorReading
    val dataStream: DataStream[SensorReading] = streaFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    // 聚合，按照温度相加
    // .keyBy(0)
    // .sum(1)
    // keyedStream要求范型为T,K；k为Tuple，当用_.id指定为String，范型K可以为String;否则需要按照Tuple定义
    val keyedStream: KeyedStream[SensorReading, String] = dataStream.keyBy(_.id)
    //  val dataSm = keyedStream.sum("temperature")
    // 聚合需要先做分区，借助分布式并行能力
    // 如果想就一个数据统计来了几个，也是每个元素 前面加一个占位 比如0 ，然后按照该占位聚合
    // 输出当前传感器最新的温度+10，而时间戳是上一次数据的时间+1
    // reduce传入二元参数，x之前聚合结果，y新来的元素
    val dataSm = keyedStream.reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))

    dataSm.print()
    //      .setParallelism(1)


    env.execute("my transform")
  }

}
