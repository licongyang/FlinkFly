package org.flink.fly.sink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import org.flink.fly.source.SensorReading

import java.util

/**
 *  查看es索引：
 *    localhost:9200/_cat/indices?v
 *  查看具体索引：
 *    localhost:9200/sensor/_search?pretty
 *
 */
object MyElasticsearchSink {

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
    // es 通过http 写入
    val httpHosts =  new java.util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    // 创建一个esSink的builder
    val  esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t)
          // 保证成一个Map或者JsonObject
          val map = new util.HashMap[String, String]()
          map.put("sensor_id", t.id)
          map.put("temperature", t.temperature.toString)
          map.put("ts", t.timestamp.toString)

          // 创建index request，准备发送数据
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            // es7 废弃type
            .`type`("readingdata")
            .source(map)

          // 发送RequestIndexer发送http请求，写入数据
          requestIndexer.add(indexRequest)
          println("data saved.")

        }
      })

    dataStream.addSink(esSinkBuilder.build())

    env.execute("elasticsearch sink")
  }

}
