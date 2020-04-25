package com.hugh.checkpoint

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-04-07 01:22
 **/
object checkPointDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(6000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
//    超时自动丢弃该cp
    env.getCheckpointConfig.setCheckpointTimeout(100000)
//    ck失败是否停止任务
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//    同时进行cp cp的最大数量
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
//    两次cp的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
//    cp外部持久化
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

//    重启策略 fail后最大三次重启 每次重启500毫秒间隔
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500))
  }

}
