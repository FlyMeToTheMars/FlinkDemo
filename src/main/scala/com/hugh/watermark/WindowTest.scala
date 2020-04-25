package com.hugh.watermark

import com.hugh.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-26 20:52
 **/
object WindowTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 不手动设置的话 默认是processTime
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    env.getConfig.setAutoWatermarkInterval(100)


    val dataStream = env.readTextFile("C:\\Users\\flyho\\OneDrive\\Flink\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")

    val sensorStream: DataStream[SensorReading] = dataStream.map(
      line => {
        val ar: Array[String] = line.split(",")
        SensorReading(ar(0).trim, ar(1).trim.toLong, ar(2).trim.toDouble)
      }
    )
//      .assignAscendingTimestamps(_.timestamp*1000)
      .assignTimestampsAndWatermarks(new MyAssigner)
    // 统计10s内的最小温度
    val minTempPerwindowStream = sensorStream
      .map(data => (data.id,data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      // 用reduce做增量聚合
      .reduce((data1,data2) => (data1._1,data1._2.min(data2._2)))


    minTempPerwindowStream.print("minTemp")
    dataStream.print("input data")

    env.execute("win test")
  }

  class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading]{
    val bound = 6000
    var maxTs = Long.MinValue

    override def getCurrentWatermark: Watermark = {
      new Watermark(maxTs - bound)
    }

    override def extractTimestamp(t: SensorReading, l: Long): Long = {
      maxTs = maxTs.max(t.timestamp*1000)
      t.timestamp * 1000
    }
  }

}
