package org.flink.fly.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.flink.fly.source.SensorReading

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 *
 *  mysql 建表语句
 *
 *  create table temperatures (
    sensor varchar(20) not null,
    temp double not null
    )
 */
object MyJDBCSink {

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
    dataStream.addSink(new MyJDBCSink())
    env.execute("JDBC sink")
  }

  // sinkfunction方法比较少，需要建立连接，则用richsinkfunction
  class MyJDBCSink() extends RichSinkFunction[SensorReading] {
    // 定义sql连接、预编译器
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    // 初始化，创建连接和预编译语句

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","root")
      insertStmt = conn.prepareStatement("INSERT INTO temperatures(sensor,temp) VALUES (?, ?)")
      updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ?  where sensor = ?")
    }

    //调用连接，执行sql
    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
      // 执行更新语句
      updateStmt.setDouble(1, value.temperature)
      updateStmt.setString(2, value.id)
      updateStmt.execute()
      // 如果update没有查到数据，那么执行插入语句
      if(updateStmt.getUpdateCount == 0){
        insertStmt.setString(1, value.id)
        insertStmt.setDouble(2, value.temperature)
        insertStmt.execute()
      }
    }
    //关闭时，做清理工作
    override def close(): Unit = {
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }
  }

}
