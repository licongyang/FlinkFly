package org.flink.fly.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.flink.fly.source.SensorReading

/**
 *
 * 开启服务端：
 * redis-server start
 * 客户端连接：
 *  redis-cli
 * 查看所有key
 * keys *
 * hgetall sensor_temperature
 * 发现以每个key输出数据，同样key数据会覆盖
 */
object MyRedisSink {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    val inputstream = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/src/main/resources/sensor.txt")

    // transform
    val dataStream = inputstream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // sink
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink(conf, new MyRedisMapper()))

    dataStream.print()

    env.execute("redis sink")

  }
}

// 定义用什么命令写什么数据
class MyRedisMapper() extends RedisMapper[SensorReading] {

  // 定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器id和温度保存成哈希表 HSET key(表名称) field（id） value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")

  }

  // 定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id

  // 定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString


}
