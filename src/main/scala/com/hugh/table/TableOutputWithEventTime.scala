package com.hugh.table

import com.hugh.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-05-17 14:05
 **/
object TableOutputWithEventTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("C:\\Users\\flyho\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")
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

    sensorTable.printSchema()
    /**
     * 输出：
     * root
     * |-- id: STRING
     * |-- ts: TIMESTAMP(3) *ROWTIME*  3代指 毫秒做单位的时间戳
     * |-- temperature: DOUBLE
     *
     *
     *
     * */
    sensorTable.toAppendStream[Row].print()

    env.execute()
  }
}


/**
 * 定义处理时间有三种方法
 * 第一种 ds直接定义出来
 * 第二种
 * 基于table schema，在类似之前连接器里面schema里面加一个
 * .filed("pt",DataTypes.TIMESTAMP(3)).proctime()   proctime
 *
 * eventTime 需要使用
 *
 * .rowtime(
 * new Rowtime()
 * .timestampsFromField("timestamp")
 * .watermarksPeriodicBounded(1000)
 *)
 *
 *
 * 第三种：在创建表的DDL中定义
 * proctime直接在DDL字段里面增加一个 pt AS PROCTIME()   blink支持
 * eventTime比较麻烦了
 * 增加两个字段
 * rt AS TO_TIMESTAMP( FROM_UNIXTIME(TS)),
 * watermark for rt as rt - interval '1' second
 *
 *
 * */