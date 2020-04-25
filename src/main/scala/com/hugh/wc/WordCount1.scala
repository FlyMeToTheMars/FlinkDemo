package com.hugh.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


/**
 * @program: FlinkDemo
 * @description: 批处理WC
 * @author: Fly.Hugh
 * @create: 2020-03-05 11:17
 **/


object WordCount {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "C:\\Users\\flyho\\IdeaProjects\\FlinkDemo\\src\\main\\resources\\hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }
}
