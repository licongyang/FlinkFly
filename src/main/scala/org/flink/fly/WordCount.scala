package org.flink.fly

import org.apache.flink.api.scala._

// 批处理 word count 程序

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "/Users/alfie/workspace/code/learn/flink-lean/FlinkFly/src/main/resources/wordcount.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 切分数据得到word，然后再word分组聚合
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
//      .groupBy(_._1) // Aggregate does not support grouping with KeySelector functions, yet.
      .sum(1)

    wordCountDataSet.print()

  }
}
