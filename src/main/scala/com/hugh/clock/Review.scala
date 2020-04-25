package com.hugh.clock

import scala.io.Source

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-04-09 02:38
 **/
object Review {
  def main(args: Array[String]): Unit = {
    var treasureTime = 0L
    val treasurestr: Iterator[String] = Source.fromFile(PathUtils.pathGet("treasure.txt")).getLines()
    for(line <- treasurestr)
    {
      treasureTime = line.toLong
    }

    val MyTreasure = treasureTime  / (1000L * 60L * 60L)

    println("精进时间： " + MyTreasure + "个小时")
  }
}
