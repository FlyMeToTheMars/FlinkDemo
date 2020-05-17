package com.hugh.table

import java.sql.Timestamp

import com.hugh.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-05-17 17:42
 **/
object WindowSQL {
  def main(args: Array[String]): Unit = {
    /**
     * 滚动窗口
     * TUMBLE(time_attr, interval)
     *        时间字段      窗口长度
     *
     * 滑动窗口
     * HOP（time_attr, interval, interval）
     *                 窗口滑动步长  窗口长度
     *
     * SESSION(time_attr, interval)
     * 会话窗口
     * */

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("C:\\Users\\flyho\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\treasure.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
    })

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,settings)
    //    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp as 'ts, 'temperature, 'et.rowtime)
    //另一种写法
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    // 窗口操作
    // 1.group 窗口，开一个10s的滚动窗口，统计每个传感器温度的数量
    val groupResultTable:Table = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id,'tw)
      .select('id,'id.count,'tw.end)

/*    val value: DataStream[(Boolean, (String, Long, Timestamp))]
    = groupResultTable.toRetractStream[(String, Long, Timestamp)]

    value.print()*/
    tableEnv.createTemporaryView("sensor",sensorTable)

    val groupResultSQLTable = tableEnv.sqlQuery(
      """
        |select
        | id,
        | count(id),
        | tumble_end(ts, interval '10' second)
        |from
        | sensor
        |group by
        | id,
        | tumble(ts, interval '10' second)
        |
        |""".stripMargin)

    groupResultSQLTable.toAppendStream[(Row)].print()

    val overResultTable = sensorTable.window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w)
      .select('id, 'ts, 'id.count over 'w, 'temperature.avg over 'w)

    val overResultSQLTable:Table = tableEnv.sqlQuery(
      """
        |select id , ts,
        | count(id) over w,
        | avg(temperature) over w
        |from sensor
        |window w as (
        |partition by id
        |order by ts
        |rows between 2 preceding and current row
        |)
        |
        |""".stripMargin)

    overResultSQLTable.toAppendStream[Row].print()

    env.execute("table ex")
  }

}
/**
 * 自定义UDF函数
 * 标量函数
 * 聚合函数
 * 表函数
 * 表聚合函数
 *
 * */