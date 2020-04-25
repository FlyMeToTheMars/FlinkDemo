package com.hugh.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.util.Random

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-05 15:55
 **/

// 温度传感器杨样例类
case class SensorReading(
                        id:String,
                        timestamp:Long,
                        temperature:Double
                        )

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.从自定义集合中读取
    val stream1 = env.fromCollection(
      List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444)
      )
    )

    // 2.从文件中读取
    val stream2 = env.readTextFile("C:\\Users\\flyho\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")

    // 3. 从元素中读取
//    env.fromElements(1, 2.0, "string").print()

    // 3.从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.229.131:9092")
    properties.setProperty("group.id", "test-consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


//    stream1.print("stream1")

//    stream2.print("stream2").setParallelism(1)

    val stream3 = env.addSource(new FlinkKafkaConsumer[String]("sensor",new SimpleStringSchema(),properties))

    // 自定义Source
    val stream4 = env.addSource(new SensorSource())

//    stream3.print()

    stream4.print()
    env.execute("source test")


  }
}

class SensorSource() extends SourceFunction[SensorReading]{

  // 定义一个flag，表示数据源是否正常运行
  var running = true

  // 正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()

    // 初始化定义一组传感器温度数据
    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 60+ rand.nextGaussian() * 20)
    )
    // 无限循环产生数据流
    while(running){
      // 在前一次温度的基础上更新温度值
      curTemp = curTemp.map(
        t => (t._1 , t._2 + rand.nextGaussian())
      )

      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
      )

      Thread.sleep(500)
    }
  }

  // 取消数据的生成
  override def cancel(): Unit = {
    running = false
  }

}