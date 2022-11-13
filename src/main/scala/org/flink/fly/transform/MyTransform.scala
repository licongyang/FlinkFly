package org.flink.fly.transform

import org.apache.flink.streaming.api.scala._
import org.flink.fly.source.SensorReading

object MyTransform {

  def main(args: Array[String]): Unit= {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val streaFromFile = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/src/main/resources/sensor.txt")

    // map ：返回DataStream ,里面范型可以改变,将之前的String转换为SensorReading
    val dataStream: DataStream[SensorReading] = streaFromFile.map( data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      // 聚合，按照温度相加
      .keyBy(0)
      .sum(1)
    //    val keyedStream: KeyedStream[SensorReading, Tuple] = dataStream.keyBy(0)
    //    val dataStream = keyedStream.sum(2)

    dataStream.print()
//      .setParallelism(1)


    env.execute("my transform")
  }

}
