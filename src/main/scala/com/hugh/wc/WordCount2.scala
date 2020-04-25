package com.hugh.wc

import org.apache.flink.streaming.api.scala._


/**
 * @program: FlinkDemo
 * @description: 流处理wc
 * @author: Fly.Hugh
 * @create: 2020-03-05 11:18
 **/
object WordCount2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 用于取消任务链 整个任务取消任务链 使用场景：某些算子已经很消耗资源，这个时候将别的算子加进来 会更加影响性能，所以将他们分开
    env.disableOperatorChaining()

    // 接受Socket文本流
    val dataStream = env.socketTextStream("localhost",7777)


    // 对每条数据进行处理
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty).disableChaining()
      .map((_,1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()

    env.execute("stream wordcount job")
  }
}
/**
    windows下面的nc命令的使用和linux下面有区别，
    windows可以在这个链接下面直接下载nc
    https://eternallybored.org/misc/netcat/
    下载后直接把nc.exe放到当前用户文件下即可使用，简单暴力
    windows命令下面是nc -l -p 7777
 **/