package com.hugh.processfunction

import com.hugh.api.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * @program: FlinkDemo
 * @description: 1s之内温度连续上升就发送警报
 * @author: Fly.Hugh
 * @create: 2020-03-26 21:58
 **/
object ProcessFunctionTest {
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

    val processStream = sensorStream.keyBy(_.id)
      .process(new MyProcess())
      .print()

    env.execute("alarm")


  }

  class MyProcess() extends KeyedProcessFunction[String, SensorReading, String] {

    //定义一个状态，用来保存上一个数据的温度值
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

    //定义一个状态，用来保存定时器的时间戳
    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTemp", classOf[Long]))

    override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
      //先取出上一个温度值
      val preTemp = lastTemp.value()

      //更新温度值
      lastTemp.update(i.temperature)

      val curTimerTs = currentTimer.value()

      //温度上升 且没有注册过定时器，注册定时器  定时器没有注册的话默认是0
      if (i.temperature > preTemp && curTimerTs == 0) {
        //获得现在的时间戳 如果是eventtime的话用currentwatermark
        val timerTs = context.timerService().currentProcessingTime() + 1000
        context.timerService().registerProcessingTimeTimer(timerTs)
        currentTimer.update(timerTs)
      } else if (preTemp > i.temperature || preTemp == 0.0) {
        //      如果温度下降，删除定时器 并且删除定时器并清空状态
        context.timerService().deleteEventTimeTimer(curTimerTs)
        currentTimer.clear()
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      //输出报警信息
      out.collect(ctx.getCurrentKey + "温度连续上升")
      currentTimer.clear()
    }
  }

}