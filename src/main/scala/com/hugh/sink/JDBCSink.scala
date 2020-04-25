package com.hugh.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.hugh.api.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-25 21:55
 **/
object JDBCSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value = env.readTextFile("C:\\Users\\flyho\\OneDrive\\Flink\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")


    val sensorStream: DataStream[SensorReading] = value.map(
      line => {
        val ar: Array[String] = line.split(",")
        SensorReading(ar(0).trim, ar(1).trim.toLong, ar(2).trim.toDouble)
      }
    )

    sensorStream.addSink(new MyJdbcSink)

    env.execute("sink test")
  }
}

  class MyJdbcSink() extends RichSinkFunction[SensorReading] {
    // 定义Sink连接 定义预编译器
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    // 初始化 创建连接和预编译语句
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      conn = DriverManager.getConnection("jdbc:mysql://192.168.229.132:3306/test","root","hadoop001")
      insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
      updateStmt = conn.prepareStatement("UPDATE temperatures SET  temp = ? WHERE sensor = ?")
    }

    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
      updateStmt.setDouble(1,value.temperature)
      updateStmt.setString(2,value.id)
      updateStmt.execute()

      if(updateStmt.getUpdateCount == 0){
        insertStmt.setString(1,value.id)
        insertStmt.setDouble(2,value.temperature)
        insertStmt.execute()
      }
    }

    override def close(): Unit = {
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }
  }


