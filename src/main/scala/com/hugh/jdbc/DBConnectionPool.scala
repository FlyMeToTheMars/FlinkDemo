package com.hugh.jdbc

import java.util.{LinkedList, ResourceBundle}
import java.sql.{Connection, DriverManager}

/**
 * @program: FlinkDemo
 * @description: 数据库连接池工具类
 * @author: Fly.Hugh
 * @create: 2020-04-07 22:10
 **/
object DBConnectionPool {
  //直接读取Resource中的文件 如果是.properties结尾 可以直接省略尾缀
  private val reader = ResourceBundle.getBundle("jdbc")
  //连接池总数
  private val max_connection = reader.getString("max_connection")
  //产生连接数
  private val connection_num = reader.getString("connection_num")
  //当前连接池已产生的连接数
  private var current_num = 0
  //连接池
  private val pools = new LinkedList[Connection]()

  private val driver = reader.getString("driver")
  private val url = reader.getString("url")
  private val username = reader.getString("username")
  private val password = reader.getString("password")

  /**
   * 加载驱动
   */
  private def before() {
    if (current_num > max_connection.toInt && pools.isEmpty()) {
      println("busyness")
      Thread.sleep(2000)
      before()
    } else {
      Class.forName(driver)
    }
  }
  /**
   * 获得连接
   */
  private def initConn(): Connection = {
    val conn = DriverManager.getConnection(url, username, password)
    //    logError(url)
    conn
  }


  /**
   * 初始化连接池
   */
  private def initConnectionPool(): LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }
  /**
   * 获得连接
   */
  def getConn():Connection={
    initConnectionPool()
    pools.poll()
  }
  /**
   * 释放连接
   */
  def releaseCon(con:Connection){
    pools.push(con)
  }
}
