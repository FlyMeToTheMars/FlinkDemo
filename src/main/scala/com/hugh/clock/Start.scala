package com.hugh.clock

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-04-09 01:56
 **/
object Start {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis.toString
    Tool.writeStart(startTime)
  }
}
