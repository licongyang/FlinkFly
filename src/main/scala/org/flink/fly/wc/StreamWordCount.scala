package org.flink.fly.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * nc -lk 7777
 * 输入： hello world
 * 输出：
 * 5> (world,1)
 * 3> (hello,1)
 * 其中前面的5，3，是标识任务并行，任务执行所在的slot号；
 * 默认并行度（槽数）为 cpu核数
 * 提交参数：-- host localhost -- port 7777
 */

// 流式 wordcount
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    //     创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    全局限制算子合并成任务链
    //    env.disableOperatorChaining()

    //     读取数据流 ： socket文本流 本地起socket
    //     nc -lk 7777
    //     netconnect listen keepalive (保持多个连接，而不会单个连接断开)
    //    val dataStream = env.socketTextStream("localhost", 7777)
    val dataStream = env.socketTextStream(host, port)

    //     对每条数据进行处理
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_ != null)
      //      强制从该算子任务开始新的任务链
      .startNewChain()
      .map((_, 1))
      //       stream 聚合需要用keybu(spark 用 reducebykey)
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()
      //       可以设置任务并行度，执行线程数量，分到那个线程slot（放开）
      .setParallelism(1)

    //     wordCountDataStream.writeAsText("/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/data")
    //    启动executor
    env.execute("streaming word count")
  }


}
