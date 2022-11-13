package org.flink.fly.transform

import org.apache.flink.streaming.api.scala._
import org.flink.fly.source.SensorReading

object MyTransform {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val streaFromFile = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/src/main/resources/sensor.txt")

    // 1.基本转换算子和聚合算子
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
    val aggStream = keyedStream.reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))

    aggStream.print()
    //      .setParallelism(1)

    // 2. 多流合并
    // split->splitstream 根据某些特性（贴了标识）把一个datastream拆分两个或者多个datastream
    // 不用直接用聚合操作，需要用select分成多个流
    // 需求：传感器数据按照温度高低，拆分成两个流
    val splitStream = dataStream.split( data => {
      if(data.temperature > 30) Seq("high") else Seq("low")
    })

    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high","low")

    high.print("high")
    low.print("low")
    all.print("all")

    // connect-> connectedStream 同流不合污,整体一个流，但各管各的（两个流数据类型可以不一样，再之后的coMap再去调整一样的）
    // coMap\coflat , 同时对流中的各个部分应用map，合并一个流（操作也可以不一样）
    // 只能两个流合并，无法后面再跟connect
    val warningStream =high.map(data => (data.id, data.temperature))
    val connectedStreams = warningStream.connect(low)

    val coMapDataStream = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "health")
    )
    coMapDataStream.print("coMapDataStream")

    // union 可以对两个或者两个以上的DataStream(流数据类型要求一样)进行合并，形成一个新的DataStream
    // 顺序可能有变化
    val unionStream = high.union(low)
    unionStream.print("union stream")





    env.execute("my transform")
  }

}
