package com.hugh.jdbc
import scala.collection.immutable

/**
 * @program: FlinkDemo
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-04-07 22:25
 **/
object UtilsTest {
  def main(args: Array[String]): Unit = {
    val stringToObjects: immutable.Seq[Map[String, Object]] = JdbcHelper.query("show tables;")
    for (e <- stringToObjects) {
      println(e)
    }

    println(JdbcHelper.query("select * from event_mapping;"))
  }
}
