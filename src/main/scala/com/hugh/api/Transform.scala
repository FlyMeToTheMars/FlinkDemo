package com.hugh.api

import org.apache.flink.streaming.api.scala._

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-06 14:59
 **/
object Transform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val streamFromFile = env.readTextFile("C:\\Users\\flyho\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")
    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

       val aggStream = dataStream .keyBy(0)
//        .sum(2)
      // 输出当前传感器最新的温度 +10，而时间戳是上一次数据的时间+1
      // x 代表了前一条数据， y代表了来到的后面一条数据
        .reduce( (x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))


    // split 流
    val splitStream = dataStream.split( data => {
      if( data.temperature > 30) Seq("high") else Seq("low")
    })

    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high","low")

    high.print("high")
    low.print("low")
    all.print("all")

    aggStream.print()

    env.execute("transform test")
  }

}
