package org.flink.fly.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.flink.fly.source.SensorReading
/**
 * 结论：
 * 1. 确定时间语义，
 * 2.开窗 （时间窗口、计数窗口；滑动滚动会话、全局）
 * 3.设置时间戳和水位线(周期性或者打点)
 *
 * 1. 滚动窗口
 * timeWindow(Time.seconds(10)) // 开时间窗口
 * 输入 ：
 * sensor_1, 1547718199, 35.80018327300259 （表示水位时间刚进行到198）
 * sensor_6, 1547718201, 15.402984393403084 （表示水位时间刚进行到200，之前的数据都到达了）
 * 这是会触发窗口计算，来的201，触发200(因为延迟1s)的窗口(窗口大小为10，左闭右开[190,200））计算，会把199计算在内
 * 因为在datastream上面分配的时间戳和水位线，在keyby之前；
 * 这里并行度为1， 只有一个任务，keyby按照key分到多个分区；不同id的数据的时间戳也会影响任务的水位线，并向后广播传递水位线
 */
/**
 * 输出结果：
 * input data> SensorReading(sensor_1,1547718199,35.80018327300259)
         input data> SensorReading(sensor_6,1547718201,15.402984393403084)
         min temp> (sensor_1,35.80018327300259)
 */

/**
 *  输入：
 *  sensor_7, 1547718202, 6.720945201171228
 *  sensor_10, 1547718205, 38.101067604893444
 *  sensor_1, 1547718206, 35.1
 *  sensor_1, 1547718207, 31.0
 *  sensor_1, 1547718208,30.
 *  sensor_1, 1547718209,29
 *  sensor_1, 1547718210,28
 *  sensor_1, 1547718201,27
 *  sensor_1, 1547718211,26
 *
 *  输出：
 *  input data> SensorReading(sensor_7,1547718202,6.720945201171228)
 *  input data> SensorReading(sensor_10,1547718205,38.101067604893444)
 *  input data> SensorReading(sensor_1,1547718206,35.1)
 *  input data> SensorReading(sensor_1,1547718207,31.0)
 *  input data> SensorReading(sensor_1,1547718208,30.0)
 *  input data> SensorReading(sensor_1,1547718209,29.0)
 *  input data> SensorReading(sensor_1,1547718210,28.0)
 *  input data> SensorReading(sensor_1,1547718201,27.0)
 *  input data> SensorReading(sensor_1,1547718211,26.0) 当输入数据时间戳为211，水位线为210，触发事件时间窗口为[200,210)计算（时间戳为该范围的数据）
 *  min temp> (sensor_6,15.402984393403084)
 *  min temp> (sensor_7,6.720945201171228)
 *  min temp> (sensor_1,27.0)
 *  min temp> (sensor_10,38.101067604893444)
 */

// 2.滑动窗口
// 统计15s内的最小温度，每5s输出一次
// .timeWindow(Time.seconds(15), Time.seconds(5))
/**
 * 输入：
 * sensor_1, 1547718199, 35.80018327300259
 * sensor_1, 1547718200, 34
 * sensor_1, 1547718201,33 ( 201时间戳的数据来了，水位线为200，触发窗口[185,200))
 *
 * 输出：
 * input data> SensorReading(sensor_1,1547718199,35.80018327300259)
 * input data> SensorReading(sensor_1,154771820,34.0)
 * input data> SensorReading(sensor_1,1547718201,33.0)
 * min temp> (sensor_1,35.80018327300259)
 *
 * 因为滑动窗口大小为15s,滑动大小为5s，延迟1s
 * 推断下一次来时间戳为206，水位线的205（206-1），触发窗口[190,205) ，窗口大小为15s，每滑动5s触发一次
 * 输入：
 * sensor_1, 1547718202,32
 * sensor_1, 1547718203,31
 * sensor_1, 1547718204,30
 * sensor_1, 1547718205,29
 * sensor_1, 1547718206,28 (来这个数据，会计算窗口[190,205)，注意不包含205，是看到204)
 *
 * 输出：
 * input data> SensorReading(sensor_1,1547718202,32.0)
 * input data> SensorReading(sensor_1,1547718203,31.0)
 * input data> SensorReading(sensor_1,1547718204,30.0)
 * input data> SensorReading(sensor_1,1547718205,29.0)
 * input data> SensorReading(sensor_1,1547718206,28.0)
 * min temp> (sensor_1,30.0)
 *

 *
 */
object MyWindow {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 指定事件时间语言
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 事件事件语义下，重新设置周期性水位线生成间隔,设置100 默认200ms； 处理事件语义，无生成水位线生成间隔（0）
    env.getConfig.setAutoWatermarkInterval(100)

//    val stream = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/src/main/resources/sensor.txt")
    val stream = env.socketTextStream("localhost", 7777)
    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      // 除了在env指定时间特性，还要在datastream上指定时间戳和水位线
      // 升序数据，不延迟，到点就触发窗口计算
//      .assignAscendingTimestamps(_.timestamp * 1000)
//      .assignTimestampsAndWatermarks(new MyPeriodicAsserigner())
      // 参数为延迟事件,这里为延迟一秒，上涨水位
      // 下一个触发窗口计算的时间戳为211（水位线210=211-1(延期1s)）,触发窗口为[210,220)时间戳的数据计算
      // 不同key分区数据（会影响水位线推进）并都会计算最小值输出
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })
    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      // window(TumblingEventTimeWindows.of(size)) or .window(TumblingProcessingTimeWindows.of(size))
      // 默认按照处理时间和滚动窗口，
      // 如果整个处理时间不够10s或者窗口没有数据，那么不会触发窗口
      // 开窗时间语义根据上面env.setStreamTimeCharacteristic来确定
      // 滚动窗口
      // 统计十秒内的最小温度
//      .timeWindow(Time.seconds(10)) // 开时间窗口
      // 滑动窗口
      // 统计15s内的最小温度，每5s输出一次
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) // 用reduce做增量聚合

    minTempPerWindowStream.print("min temp")

    dataStream.print("input data")

    env.execute("window test")
  }

  // 周期性生成水位线
  class MyPeriodicAsserigner()  extends AssignerWithPeriodicWatermarks[SensorReading]{

    // 延迟事件 毫秒
    val bound = 60 * 1000
    // 当前最大时间戳
    var maxTs = Long.MinValue
    // 生成的水位线
    // 当前所有元素的最大时间戳 - 延迟事件
    override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

    //那个字段作为事件事件
    // 参数l：表示前一个元素的时间戳
    override def extractTimestamp(t: SensorReading, l: Long): Long = {
      maxTs = maxTs.max(t.timestamp * 1000)
      t.timestamp * 1000
    }
  }
  // 随机生成水位线
  class MyPunctuateAssigner() extends AssignerWithPunctuatedWatermarks[SensorReading]{
    // l为当前元素的抽取的时间戳
    override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = new Watermark(l)

    // l为前一个元素的时间戳
    override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp * 1000
  }
}
