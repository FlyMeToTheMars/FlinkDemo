package com.hugh.clock

import java.io.{File, FileWriter}

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-04-09 01:57
 **/
object Tool {

  def writeStart(s:String) = {

    val writer = new File(PathUtils.pathGet("start.txt"))
    if(!writer.exists()) {
      writer.createNewFile()
    }

    val FileWriterIns = new FileWriter(writer)

    FileWriterIns.write(s)
    FileWriterIns.flush()
    FileWriterIns.close()
  }

  def writerTreasure(s:String) = {
    val writer1 = new File(PathUtils.pathGet("treasure.txt"))
    if(!writer1.exists()) {
      writer1.createNewFile()
    }

    val FileWriterIns = new FileWriter(writer1)

    FileWriterIns.write(s)
    FileWriterIns.flush()
    FileWriterIns.close()



  }
}
