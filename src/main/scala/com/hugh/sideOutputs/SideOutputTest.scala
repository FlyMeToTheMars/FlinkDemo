package com.hugh.sideOutputs

import com.hugh.api.SensorReading
import com.hugh.processfunction.ProcessFunctionTest.MyProcess
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-04-06 20:05
 **/
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("C:\\Users\\flyho\\OneDrive\\Flink\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")

    val sensorStream: DataStream[SensorReading] = dataStream.map(
      line => {
        val ar: Array[String] = line.split(",")
        SensorReading(ar(0).trim, ar(1).trim.toLong, ar(2).trim.toDouble)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })

    val processStream = sensorStream
      .process(new FreezingAlert())


    processStream.print("process data")
    // 打印侧输出流
    processStream.getSideOutput( new OutputTag[String]("Freezing alert")).print()

    env.execute("alarm")
  }

  // 冰点报警，如果小于32F，报警输出到侧输出流
  class FreezingAlert() extends ProcessFunction[SensorReading,SensorReading]{

    lazy val alertOutput: OutputTag[String] = new OutputTag[String]("Freezing alert")

    override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      if(i.temperature < 32.0){
        context.output(alertOutput,"freezing alert for " + i.id)
      } else {
        collector.collect(i)
      }
    }
  }
}
