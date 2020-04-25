package com.hugh.api

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-22 16:47
 **/



object multipleStreamAPI {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value = env.readTextFile("C:\\Users\\flyho\\OneDrive\\Flink\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")

    val sensorStream: DataStream[SensorReading] = value.map(
      line => {
        val ar: Array[String] = line.split(",")
        SensorReading(ar(0).trim, ar(1).trim.toLong, ar(2).trim.toDouble)
      }
    )

    val splitStream: SplitStream[SensorReading] = sensorStream.split(
      line => {
        if (line.temperature > 30) Seq("high") else Seq("low")
      }
    )

    val high: DataStream[SensorReading] = splitStream.select("high")
    val low: DataStream[SensorReading] = splitStream.select("low")

    // modify high stream
    val warning: DataStream[(String, Double)] = high.map(
      line => {
        (line.id, line.temperature)
      }
    )

    // connect
    val connectStream: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)

    val comapStream: DataStream[Product] = connectStream.map(
      lowData => (lowData._1, lowData._2, "warning"),
      warningData1 => (warningData1.id, "healthy")
    )

        env.execute("mmultipleExample")

  }



}
