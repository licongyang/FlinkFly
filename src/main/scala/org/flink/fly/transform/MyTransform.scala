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
    val dataSm = keyedStream.sum("temperature")

    dataSm.print()
    //      .setParallelism(1)


    env.execute("my transform")
  }

}
