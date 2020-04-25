package com.hugh.watermark

import com.hugh.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-26 20:18
 **/
object watermarkdemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val dataStream = env.readTextFile("C:\\Users\\flyho\\OneDrive\\Flink\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")

    val sensorStream: DataStream[SensorReading] = dataStream.map(
      line => {
        val ar: Array[String] = line.split(",")
        SensorReading(ar(0).trim, ar(1).trim.toLong, ar(2).trim.toDouble)
      }
    )

    //watermark
    sensorStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](
        Time.milliseconds(1000)
      ) {
        override def extractTimestamp(t: SensorReading): Long = {
          // 转换成秒
          t.timestamp * 1000
        }
      }
    )
  }

}
