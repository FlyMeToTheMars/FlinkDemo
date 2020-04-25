package com.hugh.state

import com.hugh.api.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @program: FlinkDemo
 * @description: 温度浮动报警
 * @author: Fly.Hugh
 * @create: 2020-04-06 21:48
 **/
object StateDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(1000)

    val dataStream = env.readTextFile("C:\\Users\\flyho\\OneDrive\\Flink\\FlinkDemo\\src\\main\\resources\\SensorReading.txt")

    val sensorStream: DataStream[SensorReading] = dataStream.map(
      line => {
        val ar: Array[String] = line.split(",")
        SensorReading(ar(0).trim, ar(1).trim.toLong, ar(2).trim.toDouble)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })

    val processStream = sensorStream.keyBy(_.id)
      .process(new MyProcess2(10.0))
      .print()

    // 直接使用flatmap
    val processStream2 = sensorStream.keyBy(_.id)
        .flatMap(new TempChangeAlert(10.0))

    //直接使用flatwithstate
    val processStream3 = sensorStream.keyBy(_.id)
        .flatMapWithState[(String,Double,Double),Double]{
              //如果没有状态，也就是没有数据来过 就将当前数据温度值存入状态
            case(input:SensorReading,None) => (List.empty,Some(input.temperature))
              //如果有状态，就应该与上次的温度值比较差值,大于阈值就报警
            case(input:SensorReading,lastTemp:Some[Double]) =>
              val diff = (input.temperature - lastTemp.get).abs
              if(diff > 10.0){
                (List((input.id,lastTemp.get,input.temperature)),Some(input.temperature))
              }else {
                (List.empty,Some(input.temperature))
              }
        }

    env.execute("alarm")
  }

  class TempChangeAlert(threshold:Double) extends RichFlatMapFunction[SensorReading, (String,Double,Double)]{

    private var lastTempState: ValueState[Double] = _

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      //获取上次的温度值
      val lastTemp: Double = lastTempState.value()
      //用当前的温度值和上次的秋茶，如果大于阈值，输出报警信息
      val diff = (value.temperature - lastTemp).abs
      if(diff > threshold){
        out.collect((value.id,lastTemp,value.temperature))
      }
      lastTempState.update(value.temperature)
    }

    override def open(parameters: Configuration): Unit = {
      //初始化的时候声明state变量
      lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
    }
  }

  class MyProcess2(threshold:Double) extends KeyedProcessFunction[String, SensorReading, (String,Double,Double)] {

    //定义一个状态，用来保存上一个数据的温度值
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

    override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, (String, Double,Double)]#Context, collector: Collector[(String, Double,Double)]): Unit = {
      //获取上次的温度值
      val lastTemp: Double = lastTempState.value()
      //用当前的温度值和上次的秋茶，如果大于阈值，输出报警信息
      val diff = (i.temperature - lastTemp).abs
      if(diff > threshold){
        collector.collect((i.id,lastTemp,i.temperature))
      }
      lastTempState.update(i.temperature)
    }
  }

}
