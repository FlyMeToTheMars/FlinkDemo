package com.hugh.watermark

import com.hugh.api.SensorReading
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-26 20:40
 **/
object customwatermark {


  class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
    // 此方法是默认周期性生成 200ms

    val bound: Long = 60 * 1000 // 延时为 1 分钟

    var maxTs: Long = Long.MinValue // 观察到的最大时间戳

    override def getCurrentWatermark: Watermark = {
      // 要注意这里使用的是减号
      // 打个比方 时间戳上10点的时间戳到了，已经十点了，这个时候watermark理所当然要比实际时间小 watermark也到十点的时候 会关闭这个窗口
      new Watermark(maxTs - bound)
    }

    override def extractTimestamp(t: SensorReading, l: Long): Long = {
      // 确保时间戳总是最大的
      maxTs = maxTs.max(t.timestamp)
      t.timestamp
    }

  }

  class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {
    val bound: Long = 60 * 1000


    override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
      // 判断什么时候应该修改watermark的时间
      if (t.id == "sensor_1") {
        new Watermark(l - bound)
      } else {
        null
      }
    }

    //
    override def extractTimestamp(t: SensorReading, l: Long): Long = {
      t.timestamp
    }
  }

}

