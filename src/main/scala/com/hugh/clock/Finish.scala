package com.hugh.clock

import scala.io.{BufferedSource, Source}

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-04-09 01:56
 **/
object Finish {
  def main(args: Array[String]): Unit = {
    var startTime = 0L
    val startstr: Iterator[String] = Source.fromFile(PathUtils.pathGet("start.txt")).getLines()
    for(line <- startstr)
    {
      startTime = line.toLong
    }

    var treasureTime =0L
    val treasurestr: Iterator[String] = Source.fromFile(PathUtils.pathGet("treasure.txt")).getLines()
    for(line <- treasurestr)
    {
      treasureTime = line.toLong
    }

    val endTime = System.currentTimeMillis

    val thisTime: Long = endTime - startTime + treasureTime

    Tool.writerTreasure(thisTime.toString)

    Review
  }
}
