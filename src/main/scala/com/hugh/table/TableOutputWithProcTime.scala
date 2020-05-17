package com.hugh.table

import com.hugh.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-05-17 13:57
 **/
object TableOutputWithProcTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.readTextFile("C:\\Users\\flyho\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,settings)

    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp, 'temperature, 'pt.proctime)
    sensorTable.printSchema()

    // ROW类型，目前了解的
    sensorTable.toAppendStream[Row].print()

    env.execute()
  }

}
